﻿using Nito.AsyncEx;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimplifiedLib
{
    public class AsyncCollectionAbortableFifoQueue<T> : IExecutableAsyncFifoQueue<T>
    {
        private AsyncCollection<AsyncWorkItem<T>> taskQueue = new AsyncCollection<AsyncWorkItem<T>>();
        private readonly CancellationToken stopProcessingToken;

        public AsyncCollectionAbortableFifoQueue(CancellationToken cancelToken)
        {
            stopProcessingToken = cancelToken;
            _ = processQueuedItems();
        }

        public Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken)
        {
            var tcs = new TaskCompletionSource<T>();
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
                            item.TaskSource.SetException(ex);
                        }
                    }
                }
                catch (Exception) { }
            }
        }
    }

    public interface IExecutableAsyncFifoQueue<T>
    {
        Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken);
    }
}
