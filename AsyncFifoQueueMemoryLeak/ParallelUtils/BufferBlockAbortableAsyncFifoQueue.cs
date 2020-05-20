using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ParallelUtils
{    

    public class BufferBlockAbortableAsyncFifoQueue<T> : IExecutableAsyncFifoQueue<T>
    {

        private readonly BufferBlock<AsyncWorkItem<T>> taskQueue = new BufferBlock<AsyncWorkItem<T>>();
        private readonly CancellationToken stopProcessingToken;

        public BufferBlockAbortableAsyncFifoQueue(CancellationToken cancelToken)
        {
            stopProcessingToken = cancelToken;
            stopProcessingToken.Register(stopProcessing);
            _ = processQueuedItems();
        }

        public Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            taskQueue.Post(new AsyncWorkItem<T>(tcs, action, cancelToken));
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
                    var item = await taskQueue.ReceiveAsync(stopProcessingToken).ConfigureAwait(false);
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
            taskQueue.Complete();
        }

    }
}
