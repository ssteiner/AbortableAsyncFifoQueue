using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelUtils
{
    /// <summary>
    /// Executable async fifo queu
    /// This implementation has one significant drawback: you cannot add a new task
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ExecutableAsyncFifoQueue<T> : IExecutableAsyncFifoQueue<T>, IDisposable
    {
        
        private BlockingCollection<AsyncWorkItem<T>> taskQueue = null;
        protected bool taskQueuedOrRunning = false;
        protected SemaphoreSlim processingLock;
        private readonly Action<string, int> logAction;

        public ExecutableAsyncFifoQueue(Action<string, int> logAction = null)
        {
            taskQueue = new BlockingCollection<AsyncWorkItem<T>>(new ConcurrentQueue<AsyncWorkItem<T>>());
            processingLock = new SemaphoreSlim(1);
            this.logAction = logAction;
        }

        public Task<T> EnqueueTask(Func<Task<T>> action)
        {
            return EnqueueTask(action, null);
        }

        public Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            try
            {
                processingLock.Wait();
                Log("enqueuing new action", 5);
                taskQueue.Add(new AsyncWorkItem<T>(tcs, action, cancelToken));
                if (!taskQueuedOrRunning)
                {
                    Log("No tasks are currently running, starting new processing task", 5);
                    taskQueuedOrRunning = true;
                    Task.Run(processQueuedItems);
                    //factory.CreateTask(() => processQueuedItems()).Start();
                }
                return tcs.Task;
            }
            finally
            {
                try
                {
                    processingLock.Release();
                }
                catch (Exception)
                {
                    Log("exception releasing processing log in EnqueueTask", 5);
                }
            }
        }

        public void Stop()
        {
            stopProcessing();
        }

        private async Task processQueuedItems()
        {
            AsyncWorkItem<T> item;
            try
            {
                await processingLock.WaitAsync().ConfigureAwait(false);
                if (taskQueue.Count == 0)
                {
                    Log("No more tasks queued for execution, aborting", 5);
                    taskQueuedOrRunning = false;
                    return;
                }
                if (taskQueue.TryTake(out item))
                {
                    if (item.CancelToken.HasValue && item.CancelToken.Value.IsCancellationRequested)
                    {
                        item.TaskSource.SetCanceled();
                    }
                    else
                    {
                        try
                        {
                            T result = await item.Action().ConfigureAwait(false);
                            item.TaskSource.SetResult(result);   // Indicate completion
                        }
                        catch (OperationCanceledException ex)
                        {
                            if (ex.CancellationToken == item.CancelToken)
                                item.TaskSource.SetCanceled();
                            else
                                item.TaskSource.SetException(ex);
                        }
                        catch (Exception ex)
                        {
                            item.TaskSource.SetException(ex);
                        }
                        finally
                        {
                            Task next = Task.Run(processQueuedItems);
                            //factory.CreateTask(() => processQueuedItems()).Start();
                        }
                    }
                }
            }
            finally
            {
                try
                {
                    processingLock.Release();
                }
                catch (Exception)
                {
                }
            }
        }

        protected void Log(string logMessage, int severity)
        {
            logAction?.Invoke(logMessage, severity);
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (taskQueue != null)
                {
                    taskQueue.CompleteAdding();
                    try
                    {
                        taskQueue.Dispose();
                    }
                    catch (ObjectDisposedException) { }
                    taskQueue = null;
                }
            }
        }

        #endregion

    }
}
