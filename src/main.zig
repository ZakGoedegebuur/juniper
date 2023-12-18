const std = @import("std");

//const Task = struct {
//    func: *const fn () void,
//
//    pub fn init(func: *const fn () void) Task {
//        return Task {
//            .func = func
//        };
//    }
//
//    pub fn run(self: *Task) void {
//        self.func();
//    }
//};
//
//const ThreadPool = struct {
//    const ShutdownPolicyEnum = enum { Continue, IfTasksFinished, IgnoreRemainingTasks };
//    allocator: std.mem.Allocator,
//    threads: std.ArrayList(std.Thread),
//    tasks: std.DoublyLinkedList(Task),
//    task_list_mtx: std.Thread.Mutex,
//
//    shutdown_policy: ShutdownPolicyEnum = .Continue,
//
//    pub fn init(allocator: std.mem.Allocator) ThreadPool {
//        return ThreadPool {
//            .allocator = allocator,
//            .threads = std.ArrayList(std.Thread).init(allocator),
//            .tasks = std.DoublyLinkedList(Task){},
//            .task_list_mtx = std.Thread.Mutex{},
//        };
//    }
//
//    const ThreadPoolConfig = struct {
//        num_threads: ?u64 = null,
//    };
//    pub fn startThreads(self: *ThreadPool, config: ThreadPoolConfig) !void {
//        try self.threads.resize(config.num_threads orelse try std.Thread.getCpuCount() + 1);
//
//        std.debug.print("Num threads: {d}\n", .{self.threads.items.len});
//        for (self.threads.items) |*thread| {
//            thread.* = try std.Thread.spawn(.{}, threadLoopFn, .{self});
//        }
//    }
//
//    const TaskCompleteEnum = enum { FinishAllTasks, FinishCurrentTask };
//    const ThreadPoolDeinitConfig = struct {
//        finish_policy: TaskCompleteEnum = .FinishAllTasks,
//    };
//    pub fn deinit(self: *ThreadPool, config: ThreadPoolDeinitConfig) void {
//        switch (config.finish_policy) {
//            .FinishAllTasks => {
//                self.shutdown_policy = .IfTasksFinished;
//                for (self.threads.items) |*thread| {
//                    thread.join();
//                }
//            },
//            .FinishCurrentTask => {
//                self.shutdown_policy = .IgnoreRemainingTasks;
//
//                self.task_list_mtx.lock();
//                while (self.tasks.pop()) |n| {
//                    self.allocator.destroy(n);
//                }
//                self.task_list_mtx.unlock();
//
//                for (self.threads.items) |*thread| {
//                    thread.join();
//                }
//
//            },
//        }
//
//        self.threads.clearAndFree();
//    }
//
//    pub fn scheduleTask(self: *ThreadPool, task: Task) !void {
//        var node = try self.allocator.create(std.DoublyLinkedList(Task).Node);
//        node.* = std.DoublyLinkedList(Task).Node{
//            .prev = undefined,
//            .next = undefined,
//            .data = task,
//        };
//
//        self.task_list_mtx.lock();
//        self.tasks.append(node);
//        self.task_list_mtx.unlock();
//    }
//
//    fn threadLoopFn(thread_pool: *ThreadPool) void {
//        const threadID: u32 = std.Thread.getCurrentId();
//
//        while (true) {
//            switch (thread_pool.shutdown_policy) {
//                .Continue => {},
//                .IfTasksFinished => {
//                    thread_pool.task_list_mtx.lock();
//                    const remaining_tasks = thread_pool.tasks.len;
//                    //std.debug.print("tasks remaining: {d}\n", .{remaining_tasks});
//                    thread_pool.task_list_mtx.unlock();
//                    if (remaining_tasks == 0) {
//                        break;
//                    }
//                },
//                .IgnoreRemainingTasks => break,
//            }
//            std.time.sleep(100 * std.time.ns_per_ms);
//
//            thread_pool.task_list_mtx.lock();
//            var node_opt = thread_pool.tasks.popFirst();
//            thread_pool.task_list_mtx.unlock();
//
//            if (node_opt) |node| {
//                node.data.run();
//                thread_pool.allocator.destroy(node);
//            } else {
//                //std.debug.print("thread {} out of available tasks!\n", .{threadID});
//            }
//        }
//
//        std.debug.print("thread {} stopping\n", .{threadID});
//    }
//};
//
//fn printTest() void {
//    std.time.sleep(100 * std.time.ns_per_ms);
//}

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

            std.debug.print("thread {d} running task or continuing\n", .{thread_ID});
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


        //if (self.tail) |tail_node| {
        //    task.next = tail_node;
        //    tail_node.prev = task;
        //    self.tail = task;
        //} else {
        //    self.tail = task;
        //    self.head = task;
        //}
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

const MsgCtx = struct {
    task: Task,
    msg: []const u8,
};

fn printContext(task: *Task) void {
    const ctx = @fieldParentPtr(MsgCtx, "task", task);
    //std.time.sleep(1000 * std.time.ns_per_ms);
    std.debug.print("message: {s}\n", .{ctx.msg});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    var tp = ThreadPool.init(allocator);
    try tp.startThreads(.{.num_threads = 4});
    {   
        var s1 = TaskSemaphore{};
        var msg_task = MsgCtx{ .msg = "OOOOOOO", .task = Task.init(printContext, &s1) };
        tp.scheduleTask(&msg_task.task);
        tp.scheduleTask(&msg_task.task);
        tp.scheduleTask(&msg_task.task);
        tp.scheduleTask(&msg_task.task);
        defer s1.wait();

        //var s2 = TaskSemaphore{};
        //var msg_task2 = MsgCtx{ .msg = "OOOOOOO", .task = Task.init(printContext, &s1) };
        //tp.scheduleTask(&msg_task2.task);
        //defer s2.wait();
    }
    tp.deinit(.{.finish_policy = .FinishAllTasks});
}
