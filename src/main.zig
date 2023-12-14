const std = @import("std");

const Task = struct {
    func: *const fn () void,

    pub fn init(func: *const fn () void) Task {
        return Task {
            .func = func
        };
    }

    pub fn run(self: *Task) void {
        self.func();
    }
};

const ThreadPool = struct {
    const ShutdownPolicyEnum = enum { Continue, IfTasksFinished, IgnoreRemainingTasks };
    allocator: std.mem.Allocator,
    threads: std.ArrayList(std.Thread),
    tasks: std.atomic.Queue(Task),

    shutdown_policy: ShutdownPolicyEnum = .Continue,

    pub fn init(allocator: std.mem.Allocator) ThreadPool {
        return ThreadPool {
            .allocator = allocator,
            .threads = std.ArrayList(std.Thread).init(allocator),
            .tasks = std.atomic.Queue(Task).init(),
        };
    }

    const ThreadPoolConfig = struct {
        num_threads: ?u64 = null,
    };
    pub fn startThreads(self: *ThreadPool, config: ThreadPoolConfig) !void {
        try self.threads.resize(config.num_threads orelse try std.Thread.getCpuCount() + 1);

        std.debug.print("Num threads: {d}\n", .{self.threads.items.len});
        for (self.threads.items) |*thread| {
            thread.* = try std.Thread.spawn(.{}, threadLoopFn, .{self});
            std.time.sleep(10 * std.time.ns_per_ms);
        }
    }

    const TaskCompleteEnum = enum { FinishAllTasks, FinishCurrentTask, CeaseImmediately };
    const ThreadPoolDeinitConfig = struct {
        finish_tasks: TaskCompleteEnum = .FinishAllTasks,
    };
    pub fn deinit(self: *ThreadPool, config: ThreadPoolDeinitConfig) void {
        switch (config.finish_tasks) {
            .FinishAllTasks => {
                self.shutdown_policy = .IfTasksFinished;
                for (self.threads.items) |*thread| {
                    thread.join();
                }
            },
            .FinishCurrentTask => {
                self.shutdown_policy = .IgnoreRemainingTasks;
                for (self.threads.items) |*thread| {
                    thread.join();
                }
            },
            .CeaseImmediately => {
                self.shutdown_policy = .IgnoreRemainingTasks;
                for (self.threads.items) |*thread| {
                    thread.detach();
                }
            }
        }

        self.threads.clearAndFree();
    }

    pub fn scheduleTask(self: *ThreadPool, task: Task) !void {
        const QNode = std.atomic.Queue(Task).Node;

        var node = try self.allocator.create(QNode);
        node.* = QNode{
            .prev = undefined,
            .next = undefined,
            .data = task,
        };

        self.tasks.put(node);
    }

    fn threadLoopFn(thread_pool: *ThreadPool) void {
        const threadID: u32 = std.Thread.getCurrentId();

        while (true) {
            if (thread_pool.shutdown) {
                std.debug.print("thread {} stopping\n", .{threadID});
                return;
            }
            //std.debug.print("thread {} looped\n", .{threadID});
            //std.time.sleep(1 * std.time.ns_per_s);
        }
    }
};

fn printTest() void {
    std.time.sleep(100 * std.time.ns_per_ms);
    std.debug.print("slept for 100ms\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    var tp = ThreadPool.init(allocator);
    try tp.startThreads(.{.num_threads = 2});
    for (0..5) |_| {
        try tp.scheduleTask(Task.init(&printTest));
    }

    var node = tp.tasks.head;
    while (node != null) {
        if (node.?.prev) |p| {
            allocator.destroy(p);
        }

        std.debug.print("Node!\n", .{});
        node = node.?.next;
    }
    allocator.destroy(tp.tasks.tail.?);

    //std.debug.print("tasks: {}\n", .{tp.tasks.dump()});
    defer tp.deinit(.{});
}