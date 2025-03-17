#include "tasksys.h"
#include<atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include <set>
#include <vector>
#include<thread>

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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->next_task_id = 0;
    this->should_terminate = false;
    
    // Create the thread pool
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread([this]() {
            while (true) {
                std::pair<IRunnable*, int> task;
                TaskID completed_bulk_id = -1;
                bool got_task = false;
                
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    
                    // Wait for work or termination signal
                    cv_workers.wait(lock, [this] {
                        return should_terminate || !ready_queue.empty();
                    });
                    
                    if (should_terminate && ready_queue.empty()) {
                        return;
                    }
                    
                    if (!ready_queue.empty()) {
                        task = ready_queue.front();
                        ready_queue.pop();
                        got_task = true;
                    }
                }
                
                if (got_task) {
                    // Execute the task
                    task.first->runTask(task.second, task.first->total_tasks);
                    
                    // Update task completion status
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        
                        // Find which bulk task this belongs to
                        for (auto& bulk_pair : bulk_tasks) {
                            if (bulk_pair.second.tasks.count(task.second) > 0 && 
                                bulk_pair.second.runnable == task.first) {
                                bulk_pair.second.completed_tasks++;
                                bulk_pair.second.tasks.erase(task.second);
                                
                                // If all tasks in this bulk are completed
                                if (bulk_pair.second.completed_tasks == bulk_pair.second.total_tasks) {
                                    completed_bulk_id = bulk_pair.first;
                                    break;
                                }
                            }
                        }
                        
                        // If a bulk task was completed, process dependent tasks
                        if (completed_bulk_id != -1) {
                            // Check waiting tasks, see if any can now be executed
                            for (auto& bulk_pair : bulk_tasks) {
                                if (!bulk_pair.second.is_ready) {
                                    // Remove completed dependency
                                    bulk_pair.second.pending_deps.erase(completed_bulk_id);
                                    
                                    // If no more dependencies, move to ready queue
                                    if (bulk_pair.second.pending_deps.empty()) {
                                        bulk_pair.second.is_ready = true;
                                        for (int i = 0; i < bulk_pair.second.total_tasks; i++) {
                                            ready_queue.push(std::make_pair(bulk_pair.second.runnable, i));
                                        }
                                    }
                                }
                            }
                            
                            // Remove completed bulk task from our tracking
                            bulk_tasks.erase(completed_bulk_id);
                            
                            // Notify sync() if all tasks are done
                            if (bulk_tasks.empty()) {
                                cv_sync.notify_all();
                            }
                            
                            // Wake up workers if there are new ready tasks
                            if (!ready_queue.empty()) {
                                cv_workers.notify_all();
                            }
                        }
                    }
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    // Signal threads to terminate and wait for them
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        should_terminate = true;
        cv_workers.notify_all();
    }
    
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Since we're asked to implement run() using runAsyncWithDeps() and sync()
    runnable->total_tasks = num_total_tasks;
    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //
    std::unique_lock<std::mutex> lock(queue_mutex);
    
    // Generate unique task ID for this bulk launch
    TaskID current_task_id = next_task_id++;
    
    // Set up the bulk task info
    BulkTaskInfo info;
    info.runnable = runnable;
    info.total_tasks = num_total_tasks;
    info.completed_tasks = 0;
    info.is_ready = true;
    runnable->total_tasks = num_total_tasks;
    
    // Add all task indices to the set
    for (int i = 0; i < num_total_tasks; i++) {
        info.tasks.insert(i);
    }
    
    // Check if all dependencies are satisfied
    for (const TaskID& dep : deps) {
        if (bulk_tasks.find(dep) != bulk_tasks.end()) {
            // This dependency is still running
            info.pending_deps.insert(dep);
            info.is_ready = false;
        }
    }
    
    // Add the bulk task to our tracking
    bulk_tasks[current_task_id] = info;
    
    // If all dependencies are satisfied, add tasks to ready queue
    if (info.is_ready) {
        for (int i = 0; i < num_total_tasks; i++) {
            ready_queue.push(std::make_pair(runnable, i));
        }
        cv_workers.notify_all();
    }
    
    return current_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(queue_mutex);
    
    // Wait until all tasks are completed
    cv_sync.wait(lock, [this] {
        return bulk_tasks.empty();
    });
}
