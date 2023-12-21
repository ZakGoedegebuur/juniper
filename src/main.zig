const std = @import("std");

const ThreadPool = struct {
    const ThreadChannel = struct { thread: std.Thread };
    const ShutdownPolicyEnum = enum { Continue, IfTasksFinished, IgnoreRemainingTasks };
    allocator: std.mem.Allocator,
    thread_channels: std.ArrayList(ThreadChannel),
    tasks: AtomicTaskQueue = AtomicTaskQueue{},
    shutdown_policy: ShutdownPolicyEnum = .Continue,

    pub fn init(allocator: std.mem.Allocator) ThreadPool {
        return ThreadPool{
            .allocator = allocator,
            .thread_channels = std.ArrayList(ThreadChannel).init(allocator),
        };
    }

    const StartThreadsConfig = struct { num_threads: ?usize = null }; 
    pub fn startThreads(self: *ThreadPool, config: StartThreadsConfig) !void {
        try self.thread_channels.resize(config.num_threads orelse try std.Thread.getCpuCount());
        for (self.thread_channels.items) |*thread_channel| {
            thread_channel.thread = try std.Thread.spawn(.{}, threadLoopFn, .{self});
        }
    }
    
    const TaskCompleteEnum = enum { FinishAllTasks, FinishCurrentTask };
    const ThreadPoolDeinitConfig = struct { finish_policy: TaskCompleteEnum = .FinishAllTasks };
    pub fn deinit(self: *ThreadPool, config: ThreadPoolDeinitConfig) void {
        self.shutdown_policy = switch(config.finish_policy) {
            .FinishAllTasks => .IfTasksFinished,
            .FinishCurrentTask => .IgnoreRemainingTasks,
        };

        for (self.thread_channels.items) |*thread_channel| {
            thread_channel.thread.join();
        }

        self.thread_channels.clearAndFree();
    }

    pub fn scheduleTask(self: *ThreadPool, task: *Task) void {
        task.semaphore_ptr.incrementWaitCount();
        self.tasks.push(task);
    }

    pub fn scheduleMultitask(self: *ThreadPool, semaphore: *TaskSemaphore, slice: anytype) ![]GContext(@TypeOf(slice)) {
        _ = semaphore;
        comptime switch (@typeInfo(@TypeOf(slice))) {
            .Pointer => {},
            else => @compileError("argument 'slice' is not of type slice"),
        };

        const num_tasks = if (slice.len >= self.thread_channels.items.len) slice.len / self.thread_channels.items.len else slice.len;

        var contexts = try self.allocator.alloc(GContext(@TypeOf(slice)), num_tasks);
        return contexts;
    }

    fn threadLoopFn(tp: *ThreadPool) void {
        const thread_ID: u32 = std.Thread.getCurrentId();
        std.debug.print("thread {d} opening\n", .{thread_ID});


        while (true) {
            switch (tp.shutdown_policy) {
                .Continue => {},
                .IfTasksFinished => {
                    if (tp.tasks.getLen() == 0) {
                        break;
                    }
                },
                .IgnoreRemainingTasks => break,
            }
            //std.time.sleep(300 * std.time.ns_per_ms);

            //std.debug.print("thread {d} running task or continuing\n", .{thread_ID});
            //std.debug.print("num tasks a: {d}\n", .{tp.tasks.len});
            if (tp.tasks.pop()) |task| {
                task.run();
            }
            //std.debug.print("num tasks b: {d}\n", .{tp.tasks.len});
            //std.time.sleep(300 * std.time.ns_per_ms);
        }

        std.debug.print("thread {d} closing\n", .{thread_ID});
    }
};

const AtomicTaskQueue = struct {
    first_in: ?*Task = null,
    last_in: ?*Task = null,
    len: usize = 0,
    mtx: std.Thread.Mutex = std.Thread.Mutex{},

    fn push(self: *AtomicTaskQueue, task: *Task) void {
        self.mtx.lock();
        task.foreward = self.last_in;
        if (self.last_in) |last| {
            last.backward = task;
        }
        self.last_in = task;
        if (self.first_in == null) {
            self.first_in = self.last_in;
        }
        self.len += 1;
        self.mtx.unlock();
    }

    fn pop(self: *AtomicTaskQueue) ?*Task {
        self.mtx.lock();
        defer self.mtx.unlock();
        //std.debug.print("ooga: \n{any}\n", .{self.first_in});
        const first = self.first_in orelse return null;
        if (first.backward) |back| {
            back.foreward = null;
        }
        
        if (self.len == 1) {
            self.first_in = null;
        } else {
            self.first_in = first.backward;
        }

        self.len -=1;
        return first;
    }

    fn getLen(self: *AtomicTaskQueue) usize {
        self.mtx.lock();
        defer self.mtx.unlock();
        return self.len;
    }
};

const TaskSemaphore = struct {
    /// num_complete is incremented every time a task with this semaphore is completed
    num_complete: usize = 0,
    /// wait_count is incremented every time a task with this semaphore is scheduled
    wait_count: usize = 0,
    mtx: std.Thread.Mutex = std.Thread.Mutex{},

    pub fn incrementWaitCount(self: *TaskSemaphore) void {
        self.mtx.lock();
        self.wait_count += 1;
        self.mtx.unlock();
    }

    pub fn incrementCompletedTasks(self: *TaskSemaphore) void {
        self.mtx.lock();
        self.num_complete += 1;
        self.mtx.unlock();
    }

    pub fn wait(self: *TaskSemaphore) void {
        while (self.num_complete < self.wait_count) {
            continue;
        }
    }
};

const Task = struct {
    foreward: ?*Task = null,
    backward: ?*Task = null,
    func: *const fn (*Task) void,
    semaphore_ptr: *TaskSemaphore,

    pub fn init(func: *const fn (*Task) void, semaphore: *TaskSemaphore) Task {
        return Task{ 
            .func = func,
            .semaphore_ptr = semaphore,
        };
    }

    pub fn run(self: *Task) void {
        @call(.auto, self.func, .{self});
        self.semaphore_ptr.incrementCompletedTasks();
    }
};

pub fn GContext(comptime T: type) type {
    return struct {
        const Self = @This();

        task: Task,
        value: T,

        fn init(comptime func: fn(*Task) void, value: T, semaphore: *TaskSemaphore) Self {
            return Self {
                .task = Task.init(func, semaphore),
                .value = value,
            };
        }

        inline fn ptrFromChild(task_ptr: *Task) *Self {
            return @fieldParentPtr(Self, "task", task_ptr);
        }
    };
}

fn printContext(task: *Task) void {
    //const ctx = @fieldParentPtr(GContext([]const u8), "task", task);
    const ctx = GContext([]const u8).ptrFromChild(task);
    std.time.sleep(1 * std.time.ns_per_s);
    std.debug.print("message: {s}\n", .{ctx.value});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    var tp = ThreadPool.init(allocator);
    try tp.startThreads(.{.num_threads = 4});
    std.time.sleep(1000 * std.time.ns_per_ms);
    {   
        var s1 = TaskSemaphore{};
        var sl = [_]u32{2312, 2313, 2314, 2315};
        var ctxts = try tp.scheduleMultitask(&s1, sl[0..2]);
        defer allocator.free(ctxts);
        var msg_task = GContext([]const u8).init(printContext, "OOOOOO", &s1);
        var msg_task2 = GContext([]const u8).init(printContext, "AAAAAA", &s1);
        tp.scheduleTask(&msg_task.task);
        tp.scheduleTask(&msg_task.task);
        tp.scheduleTask(&msg_task2.task);
        tp.scheduleTask(&msg_task.task);
        defer s1.wait();
    }
    tp.deinit(.{.finish_policy = .FinishAllTasks});
}
