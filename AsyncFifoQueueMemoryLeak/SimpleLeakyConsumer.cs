using ParallelUtils;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncFifoQueueMemoryLeak
{
    internal class SimpleLeakyConsumer
    {
        private ConcurrentDictionary<string, Lazy<IExecutableAsyncFifoQueue<bool>>> groupStateChangeExecutors = new ConcurrentDictionary<string, Lazy<IExecutableAsyncFifoQueue<bool>>>();
        private readonly ConcurrentDictionary<string, Lazy<CancellationTokenSource>> userStateChangeAborters = new ConcurrentDictionary<string, Lazy<CancellationTokenSource>>();
        protected CancellationTokenSource serverShutDownSource;
        private readonly int operationDuration = 1000;

        internal SimpleLeakyConsumer(CancellationTokenSource serverShutDownSource, int operationDuration)
        {
            this.serverShutDownSource = serverShutDownSource;
            this.operationDuration = operationDuration * 1000; // convert from seconds to milliseconds
        }

        private Lazy<IExecutableAsyncFifoQueue<bool>> getLazyQueue()
        {
            return new Lazy<IExecutableAsyncFifoQueue<bool>>(new AsyncCollectionAbortableFifoQueue<bool>(serverShutDownSource.Token));
        }

        private Lazy<CancellationTokenSource> tokenFactory()
        {
            return new Lazy<CancellationTokenSource>(new CancellationTokenSource());
        }

        internal async Task<bool> ProcessStateChange(string userId)
        {
            var executor = groupStateChangeExecutors.GetOrAdd(userId, _ => getLazyQueue()).Value;
            CancellationTokenSource oldSource = null, cancelSource = null;
            cancelSource = userStateChangeAborters.AddOrUpdate(userId, _ => tokenFactory(), (key, existingValue) =>
            {
                oldSource = existingValue.Value;
                return tokenFactory();
            }).Value;
            if (oldSource != null && !oldSource.IsCancellationRequested)
            {
                oldSource.Cancel();
                _ = delayedDispose(oldSource);
            }
            try
            {
                var token = cancelSource.Token;
                var executionTask = executor.EnqueueTask(async () => { await Task.Delay(operationDuration, token).ConfigureAwait(false); return true; }, token);
                var result = await executionTask.ConfigureAwait(false);
                if (userStateChangeAborters.TryRemove(userId, out var aborter))
                    _ = delayedDispose(aborter.Value);
                return result;
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException || e is OperationCanceledException)
                    return true;
                else
                {
                    if (userStateChangeAborters.TryRemove(userId, out var aborter))
                        _ = delayedDispose(aborter.Value);
                    return false;
                }
            }
        }

        private async Task delayedDispose(CancellationTokenSource src)
        {
            try
            {
                await Task.Delay(20 * 1000).ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    src.Dispose();
                }
                catch (ObjectDisposedException) { }
            }
        }

    }
}
