using System;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelUtils
{
    internal class AsyncWorkItem<T>
    {
        public readonly TaskCompletionSource<T> TaskSource;
        public readonly Func<Task<T>> Action;
        public readonly CancellationToken? CancelToken;

        public AsyncWorkItem(TaskCompletionSource<T> taskSource, Func<Task<T>> action, CancellationToken? cancelToken)
        {
            TaskSource = taskSource;
            Action = action;
            CancelToken = cancelToken;
        }
    }

    public interface IExecutableAsyncFifoQueue<T>
    {
        Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken);

        void Stop();
    }
}
