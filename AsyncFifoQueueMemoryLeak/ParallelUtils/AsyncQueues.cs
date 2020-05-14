using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelUtils
{
    internal class AsyncWorkItem<T>
    {
        public readonly TaskCompletionSource<T> TaskSource;
        public readonly Func<Task<T>> Action;
        public readonly CancellationToken? CancelToken;
        public readonly T ActionParameter;

        public AsyncWorkItem(TaskCompletionSource<T> taskSource, Func<Task<T>> action, CancellationToken? cancelToken)
        {
            TaskSource = taskSource;
            Action = action;
            CancelToken = cancelToken;
        }

        public AsyncWorkItem(TaskCompletionSource<T> taskSource, Func<Task<T>> action, CancellationToken? cancelToken, T actionParameter)
            : this(taskSource, action, cancelToken)
        {
            ActionParameter = actionParameter;
        }
    }

    public interface IExecutableAsyncFifoQueue<T> : IDisposable
    {
        Task<T> EnqueueTask(Func<Task<T>> action);

        Task<T> EnqueueTask(Func<Task<T>> action, CancellationToken? cancelToken);
    }
}
