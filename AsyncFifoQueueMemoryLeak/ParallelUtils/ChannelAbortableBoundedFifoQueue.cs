using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ParallelUtils
{
    public class ChannelAbortableBoundedFifoQueue<T>: IExecutableAsyncFifoQueue<T>
    {

        private Channel<AsyncWorkItem<T>> taskQueue;
        private readonly Action<string, int> logAction;
        private readonly CancellationToken stopProcessingToken;

        public ChannelAbortableBoundedFifoQueue(CancellationToken cancelToken, Action<string, int> logAction = null)
        {
            taskQueue = Channel.CreateBounded<AsyncWorkItem<T>>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
            stopProcessingToken = cancelToken;
            stopProcessingToken.Register(stopProcessing);
            this.logAction = logAction;
            _ = processQueuedItems();
        }

        private void stopProcessing()
        {
            taskQueue.Writer.TryComplete();
        }

        public void Dispose()
        {

        }

        public Task<T> EnqueueTask(Func<Task<T>> action)
        {
            return EnqueueTask(action, null);
        }

        public Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            Log($"Adding new task", 5);
            var item = new AsyncWorkItem<T>(tcs, action, cancelToken);
            bool written = taskQueue.Writer.TryWrite(item);
            if (!written)
                Log($"Unable to add new item", 3);
            return tcs.Task;
        }

        protected virtual async Task processQueuedItems()
        {
            while (!stopProcessingToken.IsCancellationRequested)
            {
                try
                {
                    var item = await taskQueue.Reader.ReadAsync(stopProcessingToken).ConfigureAwait(false);
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
