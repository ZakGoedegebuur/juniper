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
    tasks: std.DoublyLinkedList(Task),
    task_list_mtx: std.Thread.Mutex,

    shutdown_policy: ShutdownPolicyEnum = .Continue,

    pub fn init(allocator: std.mem.Allocator) ThreadPool {
        return ThreadPool {
            .allocator = allocator,
            .threads = std.ArrayList(std.Thread).init(allocator),
            .tasks = std.DoublyLinkedList(Task){},
            .task_list_mtx = std.Thread.Mutex{},
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
        }
    }

    const TaskCompleteEnum = enum { FinishAllTasks, FinishCurrentTask, CeaseImmediately };
    const ThreadPoolDeinitConfig = struct {
        finish_policy: TaskCompleteEnum = .FinishAllTasks,
    };
    pub fn deinit(self: *ThreadPool, config: ThreadPoolDeinitConfig) void {
        switch (config.finish_policy) {
            .FinishAllTasks => {
                self.shutdown_policy = .IfTasksFinished;
                for (self.threads.items) |*thread| {
                    thread.join();
                }
            },
            .FinishCurrentTask => {
                self.shutdown_policy = .IgnoreRemainingTasks;

                self.task_list_mtx.lock();
                while (self.tasks.pop()) |n| {
                    self.allocator.destroy(n);
                }
                self.task_list_mtx.unlock();

                for (self.threads.items) |*thread| {
                    thread.join();
                }

            },
            .CeaseImmediately => {
                self.shutdown_policy = .IgnoreRemainingTasks;

                self.task_list_mtx.lock();
                while (self.tasks.pop()) |n| {
                    self.allocator.destroy(n);
                }
                self.task_list_mtx.unlock();
                
                for (self.threads.items) |*thread| {
                    thread.detach();
                }
            }
        }

        self.threads.clearAndFree();
    }

    pub fn scheduleTask(self: *ThreadPool, task: Task) !void {
        var node = try self.allocator.create(std.DoublyLinkedList(Task).Node);
        node.* = std.DoublyLinkedList(Task).Node{
            .prev = undefined,
            .next = undefined,
            .data = task,
        };

        self.task_list_mtx.lock();
        self.tasks.append(node);
        self.task_list_mtx.unlock();
    }

    fn threadLoopFn(thread_pool: *ThreadPool) void {
        const threadID: u32 = std.Thread.getCurrentId();

        while (true) {
            switch (thread_pool.shutdown_policy) {
                .Continue => {},
                .IfTasksFinished => {
                    thread_pool.task_list_mtx.lock();
                    const remaining_tasks = thread_pool.tasks.len;
                    //std.debug.print("tasks remaining: {d}\n", .{remaining_tasks});
                    thread_pool.task_list_mtx.unlock();
                    if (remaining_tasks == 0) {
                        break;
                    }
                },
                .IgnoreRemainingTasks => break,
            }
            std.debug.print("thread {} continuing\n", .{threadID});

            thread_pool.task_list_mtx.lock();
            var node_opt = thread_pool.tasks.popFirst();

            if (node_opt) |node| {
                // must make a copy of the task and destroy it before unlocking the mutex or if the thread is detached the memory may leak
                var owned_task = node.data; 
                thread_pool.allocator.destroy(node);
                thread_pool.task_list_mtx.unlock();

                owned_task.run();
            } else {
                thread_pool.task_list_mtx.unlock();
                //std.debug.print("thread {} out of available tasks!\n", .{threadID});
            }
        }
        
        std.debug.print("thread {} stopping\n", .{threadID});
    }
};

fn printTest() void {
    std.time.sleep(100 * std.time.ns_per_ms);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    var tp = ThreadPool.init(allocator);
    for (0..10) |_| {
        try tp.scheduleTask(Task.init(&printTest));
    }
    try tp.startThreads(.{.num_threads = 2});

    std.time.sleep(2 * std.time.ns_per_s);
    tp.deinit(.{.finish_policy = .FinishAllTasks});
}