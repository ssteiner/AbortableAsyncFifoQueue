using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelUtils
{
    /// <summary>
    /// this approach is generating too much CPU load since it keeps looping if there's no data in the queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ExecutableAsyncFifoQueue2<T> : IExecutableAsyncFifoQueue<T>
    {

        private BlockingCollection<AsyncWorkItem<T>> taskQueue = null;
        private readonly Action<string, int> logAction;
        private readonly CancellationToken stopProcessingToken;

        public ExecutableAsyncFifoQueue2(CancellationToken cancelToken, Action<string, int> logAction = null)
        {
            taskQueue = new BlockingCollection<AsyncWorkItem<T>>(new ConcurrentQueue<AsyncWorkItem<T>>());
            stopProcessingToken = cancelToken;
            stopProcessingToken.Register(stopProcessing);
            this.logAction = logAction;
            _ = processQueuedItems();
        }

        public void Stop()
        {
            stopProcessing();
        }

        public Task<T> EnqueueTask(Func<Task<T>> action)
        {
            return EnqueueTask(action, null);
        }

        public Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            Log($"Adding new task, nb items in queue: {taskQueue.Count}", 5);
            taskQueue.Add(new AsyncWorkItem<T>(tcs, action, cancelToken));
            return tcs.Task;
        }

        private async Task processQueuedItems()
        {
            while (!taskQueue.IsCompleted)
            {
                try
                {
                    if (taskQueue.TryTake(out AsyncWorkItem<T> item))
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
                        }
                    }
                }
                catch (Exception e)
                {
                    Log($"exception processing a task: {e.Message} at {e.StackTrace}", 2);
                }
            }
        }

        private void stopProcessing()
        {
            taskQueue.CompleteAdding();
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
                    try
                    {
                        taskQueue.CompleteAdding();
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
