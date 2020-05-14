using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ParallelUtils
{    

    public class BufferBlockAbortableAsyncFifoQueue<T> : IExecutableAsyncFifoQueue<T>
    {

        private BufferBlock<AsyncWorkItem<T>> buffer;
        private readonly CancellationToken stopProcessingToken;

        public BufferBlockAbortableAsyncFifoQueue(CancellationToken cancelToken)
        {
            buffer = new BufferBlock<AsyncWorkItem<T>>();
            stopProcessingToken = cancelToken;
            stopProcessingToken.Register(stopProcessing);
            _ = processQueuedItems();
        }

        #region disposing

        private void stopProcessing()
        {
            buffer.Complete();
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
                if (buffer != null)
                {
                    buffer.Complete();
                    buffer = null;
                }
            }
        }

        #endregion

        public virtual Task<T> EnqueueTask(Func<Task<T>> action)
        {
            return EnqueueTask(action, null);
        }

        public virtual Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
            log($"Adding new task, nb items in buffer: {buffer.Count}", 5);
            buffer.Post(new AsyncWorkItem<T>(tcs, action, cancelToken));
            return tcs.Task;
        }

        protected virtual async Task processQueuedItems()
        {
            AsyncWorkItem<T> item;
            while (!stopProcessingToken.IsCancellationRequested)
            {
                try
                {
                    item = await buffer.ReceiveAsync(stopProcessingToken).ConfigureAwait(false);
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
                catch (TaskCanceledException) // wait task for receive was aborted.. 
                {

                }
                catch (Exception e)
                {
                    log($"exception processing a task: {e.Message} at {e.StackTrace}", 2);
                }
            }
        }

        private void log(string message, int severity)
        {
            //Console.WriteLine($"{DateTime.Now:HH:mm:ss} {message}");
        }

    }
}
