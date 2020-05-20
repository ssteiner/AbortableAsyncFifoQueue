using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ParallelUtils
{
    public class ChannelAbortableBoundedFifoQueue<T>: IExecutableAsyncFifoQueue<T>
    {

        private Channel<AsyncWorkItem<T>> taskQueue = Channel.CreateBounded<AsyncWorkItem<T>>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
        private readonly CancellationToken stopProcessingToken;

        public ChannelAbortableBoundedFifoQueue(CancellationToken cancelToken)
        {
            stopProcessingToken = cancelToken;
            stopProcessingToken.Register(stopProcessing);
            _ = processQueuedItems();
        }

        public Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            var item = new AsyncWorkItem<T>(tcs, action, cancelToken);
            _ = taskQueue.Writer.TryWrite(item);
            return tcs.Task;
        }

        public void Stop()
        {
            stopProcessing();
        }

        private async Task processQueuedItems()
        {
            while (!stopProcessingToken.IsCancellationRequested)
            {
                try
                {
                    var item = await taskQueue.Reader.ReadAsync(stopProcessingToken).ConfigureAwait(false);
                    if (item.CancelToken.HasValue && item.CancelToken.Value.IsCancellationRequested)
                        item.TaskSource.SetCanceled();
                    else
                    {
                        try
                        {
                            T result = await item.Action().ConfigureAwait(false);
                            item.TaskSource.SetResult(result);   // Indicate completion
                        }
                        catch (Exception ex)
                        {
                            if (ex is OperationCanceledException && ((OperationCanceledException)ex).CancellationToken == item.CancelToken)
                                item.TaskSource.SetCanceled();
                            else
                                item.TaskSource.SetException(ex);
                        }
                    }
                }
                catch (Exception) { }
            }
        }

        private void stopProcessing()
        {
            taskQueue.Writer.Complete();
        }

    }
}
