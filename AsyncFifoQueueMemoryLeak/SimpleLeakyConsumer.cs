using AsyncFifoQueueMemoryLeak.Models;
using ParallelUtils;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncFifoQueueMemoryLeak
{
    internal class SimpleLeakyConsumer
    {
        private ConcurrentDictionary<string, IExecutableAsyncFifoQueue<bool>> groupStateChangeExecutors = new ConcurrentDictionary<string, IExecutableAsyncFifoQueue<bool>>();
        private readonly ConcurrentDictionary<string, CancellationTokenSource> userStateChangeAborters = new ConcurrentDictionary<string, CancellationTokenSource>();
        protected CancellationTokenSource serverShutDownSource;
        private readonly int operationDuration = 1000;

        internal SimpleLeakyConsumer(CancellationTokenSource serverShutDownSource, int operationDuration)
        {
            this.serverShutDownSource = serverShutDownSource;
            this.operationDuration = operationDuration * 1000; // convert from seconds to milliseconds
        }

        internal async Task<bool> ProcessStateChange(UserState state, User user)
        {
            var executor = groupStateChangeExecutors.GetOrAdd(user.UserId, new AsyncCollectionAbortableFifoQueue<bool>(serverShutDownSource.Token));
            CancellationTokenSource oldSource = null;
            using (var cancelSource = userStateChangeAborters.AddOrUpdate(user.UserId, new CancellationTokenSource(), (key, existingValue) =>
            {
                oldSource = existingValue;
                return new CancellationTokenSource();
            }))
            {
                if (oldSource != null && !oldSource.IsCancellationRequested)
                {
                    oldSource.Cancel();
                    _ = delayedDispose(oldSource);
                }
                try
                {
                    var executionTask = executor.EnqueueTask(async () => { await Task.Delay(operationDuration, cancelSource.Token).ConfigureAwait(false); return true; }, cancelSource.Token);
                    var result = await executionTask.ConfigureAwait(false);
                    userStateChangeAborters.TryRemove(user.UserId, out var aborter);
                    return result;
                }
                catch (Exception e)
                {
                    if (e is TaskCanceledException || e is OperationCanceledException)
                        return true;
                    else
                    {
                        userStateChangeAborters.TryRemove(user.UserId, out var aborter);
                        return false;
                    }
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
