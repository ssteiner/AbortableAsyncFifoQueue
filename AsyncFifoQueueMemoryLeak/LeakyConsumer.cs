using AsyncFifoQueueMemoryLeak.Models;
using ParallelUtils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncFifoQueueMemoryLeak
{
    internal class LeakyConsumer
    {

        private readonly ConcurrentDictionary<string, UserState> userPresenceStates;
        private ConcurrentDictionary<string, IExecutableAsyncFifoQueue<bool>> groupStateChangeExecutors;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> userStateChangeAborters;

        private readonly List<User> users;
        protected CancellationTokenSource serverShutDownSource;
        private readonly int operationDuration = 1000;

        internal LeakyConsumer(List<User> users, CancellationTokenSource serverShutDownSource, int operationDuration)
        {
            userPresenceStates = new ConcurrentDictionary<string, UserState>();
            userStateChangeAborters = new ConcurrentDictionary<string, CancellationTokenSource>();
            groupStateChangeExecutors = new ConcurrentDictionary<string, IExecutableAsyncFifoQueue<bool>>();

            this.serverShutDownSource = serverShutDownSource;
            this.users = users;
            this.operationDuration = operationDuration * 1000; // convert from seconds to milliseconds
        }

        internal void Stop()
        {
            serverShutDownSource.Cancel();
        }

        internal async Task ProcessStateChange(UserState state, User user)
        {
            UserState previousState = UserState.Offline;
            bool previousStateAvailable = false;
            UserState newState = userPresenceStates.AddOrUpdate(user.UserId, state, (key, existingValue) =>
            {
                previousStateAvailable = true;
                previousState = existingValue;
                return state;
            });
            if (previousState != newState || !previousStateAvailable)
            {
                if (previousState != newState)
                    Log($"State of {user.UserId} has changed from {previousState} to {newState}", 4);
                else if (!previousStateAvailable)
                    Log($"Processing state of {user.UserId} because no previous state was available, state: {newState}", 4);
                await processUseStateUpdateAsync(user, newState, previousState).ConfigureAwait(false);
            }
            else
            {
                Log($"Received a state update for {user.UserId} that contains no change in state ({newState}), skipping processing", 5);
            }
        }

        private IExecutableAsyncFifoQueue<bool> getQueue()
        {
            return new ChannelAbortableBoundedFifoQueue<bool>(serverShutDownSource.Token);
            // using AsyncCollection
            //return new AsyncCollectionAbortableFifoQueue<bool>(serverShutDownSource.Token);
            // using BufferBlock
            // return new BufferBlockAbortableAsyncFifoQueue<bool>(serverShutDownSource.Token);
        }

        internal async Task<bool> processUseStateUpdateAsync(User user, UserState state, UserState previousState)
        {
            var executor = groupStateChangeExecutors.GetOrAdd(user.UserId, getQueue());
            CancellationTokenSource oldSource = null;
            using (var cancelSource = userStateChangeAborters.AddOrUpdate(user.UserId, new CancellationTokenSource(), (key, existingValue) =>
            {
                oldSource = existingValue;
                return new CancellationTokenSource();
            }))
            {
                if (oldSource != null)
                {
                    Log($"Cancelling execution of {nameof(processUseStateUpdateAsync)} for user {user.UserId} because there's a new state: {state}", 4);
                    cancelAndDispose(oldSource);
                    oldSource = null;
                }
                Log($"Enqueuing presence state update for user {user.UserId}", 5);
                try
                {
                    var executionTask = executor.EnqueueTask(() => processUserStateUpdateAsync(user, state, previousState,
                        cancelSource.Token));
                    if (cancelSource.Token.IsCancellationRequested)
                        return false;
                    var result = await executionTask.ConfigureAwait(false);
                    userStateChangeAborters.TryRemove(user.UserId, out var aborter);
                    return result;
                }
                catch (Exception e)
                {
                    if (e is TaskCanceledException || e is OperationCanceledException)
                    {
                        Log($"Processing of presence state update for user {user.UserId} was aborted because another state came in", 4);
                        return true;
                    }
                    else
                    {
                        Log($"Something went wrong in {nameof(processUseStateUpdateAsync)}: {e.Message}", 2);
                        userStateChangeAborters.TryRemove(user.UserId, out var aborter);
                        return false;
                    }
                }
            }
        }

        private async Task<bool> processUserStateUpdateAsync(User user, UserState state, UserState previousState,
            CancellationToken token)
        {
            if (user.Groups != null && user.Groups.Count > 0)//only do this if the user does have any groups
            {
                if (state != UserState.OnThePhone)
                {
                    Log($"User {user.UserId} has the following groups: {string.Join(",", user.Groups.Select(x => x.Name))}", 4);
                    await Task.Delay(operationDuration, token).ConfigureAwait(false);
                }
                else
                    Log($"User {user.UserId} is changing from {previousState} to {state}, skipping update", 4);
            }
            if (token.IsCancellationRequested)
            {
                Log($"User state processing aborted at position 1", 4);
                return false;
            }
            await Task.Delay(operationDuration, token).ConfigureAwait(false);
            Log($"Successfully processed state change of {user.UserId} from {previousState} to {state}", 4);
            return true;
        }

        #region helper methods

        private void cancelAndDispose(CancellationTokenSource source)
        {
            try
            {
                if (!source.IsCancellationRequested)
                    source.Cancel();
            }
            catch (Exception) { }
            _ = delayedDispose(source);
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

        internal void Log(string message, int severity)
        {
            string severityString = null;
            switch (severity)
            {
                case 1:
                    severityString = "FATAL";
                    break;
                case 2:
                    severityString = "ERROR";
                    break;
                case 3:
                    severityString = "WARN";
                    break;
                case 4:
                    severityString = "INFO";
                    break;
                default:
                    severityString = "DEBUG";
                    break;
            }
            Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} | {severityString} | {message}");
        }

        #endregion

    }
}
