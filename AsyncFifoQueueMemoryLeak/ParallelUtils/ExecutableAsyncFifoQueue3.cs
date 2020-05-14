using Nito.AsyncEx;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelUtils
{
    public class ExecutableAsyncFifoQueue3<T> : IExecutableAsyncFifoQueue<T>
    {
        private AsyncCollection<AsyncWorkItem<T>> taskQueue;
        private readonly Action<string, int> logAction;
        private readonly CancellationToken stopProcessingToken;

        public ExecutableAsyncFifoQueue3(CancellationToken cancelToken, Action<string, int> logAction = null)
        {
            taskQueue = new AsyncCollection<AsyncWorkItem<T>>();
            stopProcessingToken = cancelToken;
            stopProcessingToken.Register(stopProcessing);
            this.logAction = logAction;
            _ = processQueuedItems();
        }

        private void stopProcessing()
        {
            taskQueue.CompleteAdding();
        }

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
                    taskQueue = null;
                }
            }
        }

        public virtual Task<T> EnqueueTask(Func<Task<T>> action)
        {
            return EnqueueTask(action, null);
        }

        public virtual Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            Log($"Adding new task", 5);
            var item = new AsyncWorkItem<T>(tcs, action, cancelToken);
            taskQueue.Add(item);
            return tcs.Task;
        }

        protected virtual async Task processQueuedItems()
        {
            while (!stopProcessingToken.IsCancellationRequested)
            {
                try
                {
                    var item = await taskQueue.TakeAsync(stopProcessingToken).ConfigureAwait(false);
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
                catch (Exception e)
                {
                    Log($"exception processing a task: {e.Message} at {e.StackTrace}", 2);
                }
            }
        }

        protected void Log(string logMessage, int severity)
        {
            logAction?.Invoke(logMessage, severity);
        }
    }
}
