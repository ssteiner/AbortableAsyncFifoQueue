using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimplifiedLib
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
    
}
