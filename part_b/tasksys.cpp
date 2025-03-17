#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    running = false;
    shutdown = false;
    next_task_id = 0;
    current_runnable = nullptr;
    total_tasks = 0;
    next_task = 0;
    tasks_completed = 0;

    // Create worker threads
    for (int i = 0; i < num_threads; i++) {
        workers.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::worker_thread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mtx);
        shutdown = true;
    }
    cv_task.notify_all();
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    while (true) {
        TaskID batch_id;
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv_task.wait(lock, [this] {
                return shutdown || !ready_queue.empty();
            });

            if (shutdown) {
                break;
            }

            if (!ready_queue.empty()) {
                batch_id = ready_queue.front();
                ready_queue.pop();
            } else {
                continue;
            }
        }

        // Get the task batch
        BulkTask& batch = task_graph[batch_id];
        if (batch.runnable != nullptr) {
            // Execute all tasks in the batch
            for (int i = 0; i < batch.num_tasks; i++) {
                batch.runnable->runTask(i, batch.num_tasks);
                
                std::unique_lock<std::mutex> lock(mtx);
                batch.tasks_completed++;
                if (batch.tasks_completed == batch.num_tasks) {
                    batch.completed = true;
                    update_dependents(batch_id);
                    cv_completion.notify_one();
                }
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::process_ready_tasks() {
    for (auto& pair : task_graph) {
        BulkTask& task = pair.second;
        if (!task.ready && !task.completed) {
            bool all_deps_completed = true;
            for (TaskID dep : task.dependencies) {
                if (task_graph.find(dep) != task_graph.end() && !task_graph[dep].completed) {
                    all_deps_completed = false;
                    break;
                }
            }
            if (all_deps_completed) {
                task.ready = true;
                ready_queue.push(pair.first);
                cv_task.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::update_dependents(TaskID completed_task_id) {
    for (TaskID dependent_id : task_graph[completed_task_id].dependents) {
        BulkTask& dependent = task_graph[dependent_id];
        dependent.tasks_completed++;
        if (dependent.tasks_completed == dependent.num_tasks) {
            dependent.completed = true;
            process_ready_tasks();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    if (num_total_tasks <= 0) {
        return -1;
    }

    TaskID task_id = next_task_id++;
    BulkTask task;
    task.runnable = runnable;
    task.num_tasks = num_total_tasks;
    task.tasks_completed = 0;
    task.dependencies = deps;
    task.ready = deps.empty();
    task.completed = false;

    {
        std::unique_lock<std::mutex> lock(mtx);
        task_graph[task_id] = task;

        // Update dependency relationships
        for (TaskID dep : deps) {
            if (task_graph.find(dep) != task_graph.end()) {
                task_graph[dep].dependents.push_back(task_id);
            }
        }

        if (task.ready) {
            ready_queue.push(task_id);
            cv_task.notify_one();
        }
    }

    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mtx);
    cv_completion.wait(lock, [this] {
        for (const auto& pair : task_graph) {
            if (!pair.second.completed) {
                return false;
            }
        }
        return true;
    });
}
