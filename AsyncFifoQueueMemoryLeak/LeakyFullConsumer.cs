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
    /// <summary>
    /// this class takes submitted UserStatus, and performs some operations based on it
    /// a status change of a user may trigger 0 up to 1 + (number of Teams the user is a member of) operations which are executed in parallel
    /// </summary>
    internal class LeakyFullConsumer
    {

        private readonly ConcurrentDictionary<string, UserState> userPresenceStates;
        private readonly ConcurrentDictionary<string, UserState> groupPresenceStates;
        private readonly ConcurrentDictionary<string, Lazy<SemaphoreSlim>> userLocks;
        private ConcurrentDictionary<string, Lazy<IExecutableAsyncFifoQueue<bool>>> groupStateChangeExecutors;
        private readonly ConcurrentDictionary<string, Lazy<CancellationTokenSource>> userStateChangeAborters;
        protected ConcurrentDictionary<string, Lazy<CancellationTokenSource>> groupStateChangeAborters;
        private readonly ConcurrentDictionary<string, Lazy<SemaphoreSlim>> groupLocks;

        protected CancellationTokenSource serverShutDownSource;

        private int operationDuration = 1000, maximumNumberOfParallelOperations = 20;

        private readonly List<User> users;

        private readonly List<Team> teams;

        internal bool DisableTeams { get; set; }

        internal LeakyFullConsumer(List<User> users, List<Team> teams, CancellationTokenSource serverShutDownSource, int operationDuration)
        {
            userPresenceStates = new ConcurrentDictionary<string, UserState>();
            groupPresenceStates = new ConcurrentDictionary<string, UserState>();
            userStateChangeAborters = new ConcurrentDictionary<string, Lazy<CancellationTokenSource>>();
            groupStateChangeAborters = new ConcurrentDictionary<string, Lazy<CancellationTokenSource>>();
            groupStateChangeExecutors = new ConcurrentDictionary<string, Lazy<IExecutableAsyncFifoQueue<bool>>>();
            userLocks = new ConcurrentDictionary<string, Lazy<SemaphoreSlim>>();
            users.ForEach(u => userLocks.TryAdd(u.UserId, new Lazy<SemaphoreSlim>(new SemaphoreSlim(1))));
            groupLocks = new ConcurrentDictionary<string, Lazy<SemaphoreSlim>>();
            if (teams != null)
                teams.ForEach(t => groupLocks.TryAdd(t.UserId, new Lazy<SemaphoreSlim>(new SemaphoreSlim(1))));

            this.serverShutDownSource = serverShutDownSource;
            this.teams = teams;
            this.users = users;
            this.operationDuration = operationDuration * 1000; // convert from seconds to milliseconds
        }

        internal void Stop()
        {
            serverShutDownSource.Cancel();
        }

        internal async Task ProcessStateChange(UserState state, string userId, bool forceSend = false)
        {
            UserState previousState = UserState.Offline;
            bool previousStateAvailable = false;
            User user = users.FirstOrDefault(u => u.UserId == userId);
            UserState newState = userPresenceStates.AddOrUpdate(userId, state, (key, existingValue) =>
            {
                previousStateAvailable = true;
                previousState = existingValue;
                return state;
            });
            if (previousState != newState || !previousStateAvailable)
            {
                if (previousState != newState)
                    Log($"Presence state of {userId} has changed from {previousState} to {newState}", 4);
                else if (!previousStateAvailable)
                    Log($"Processing presence state of {userId} because no previous state was available, state: {newState}", 4);
                await processPresenceStateChange(userId, newState, previousState).ConfigureAwait(false);
            }
            else
            {
                Log($"Received a presence state update for {userId} that contains no change in simplified state ({newState}), skipping processing", 5);
            }
        }

        internal async Task processPresenceStateChange(string entity, UserState state, UserState previousState, bool isInitialState = false)
        {
            List<Task> processTasks = new List<Task>();
            var user = users.FirstOrDefault(u => u.UserId == entity);
            if (user != null)
            {
                Task t = processUserPresenceUpdateAsync(user, state, previousState, isInitialState);
                processTasks.Add(t);
            }
            else
            {
                Log($"No known user for presence state update for entity {entity}", 3);
                return;
            }
            if (!DisableTeams)
            {
                IEnumerable<Team> affectedTeams = teams.Where(t => t.Members != null && t.Members.Any(x => x == user.UserId));
                if (affectedTeams.Count() > 0)
                {
                    //foreach (var team in affectedTeams)
                    //{
                    //    var task = processTeamPresenceUpdate(team, $"{$"user {entity} status changed to "}{state}");
                    //    processTasks.Add(task);
                    //}
                    Task updateTask = affectedTeams.ForEachAsync(maximumNumberOfParallelOperations, (team) =>
                    {
                        return processTeamPresenceUpdate(team, $"{$"user {entity} status changed to "}{state}");
                    });
                    processTasks.Add(updateTask);
                }
            }
            try
            {
                await Task.WhenAll(processTasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Log($"Something went wrong in {nameof(processPresenceStateChange)}: {e.Message}", 2);
            }
        }

        internal async Task<bool> processTeamPresenceUpdate(Team team, string reason, bool force = false)
        {
            var executor = getAsyncFifoExecutor(team.UserId);
            CancellationTokenSource oldSource = null, cancelSource = null;
            cancelSource = groupStateChangeAborters.AddOrUpdate(team.UserId, _ => tokenFactory(team.UserId, false), (key, existingValue) =>
            {
                oldSource = existingValue.Value;
                return tokenFactory(key, false);
            }).Value;
            if (oldSource != null)
            {
                Log($"Cancelling execution of {nameof(processTeamPresenceUpdate)} for team {team.Name} because there's a new call to {nameof(processTeamPresenceUpdate)}", 4);
                cancel(oldSource, team.UserId);
                //cancelAndDispose(oldSource, team.UserId);
            }
            Log($"Enqueuing presence state update for team {team.Name}, reason: {reason} for source {cancelSource.GetHashCode()}", 5);
            try
            {
                if (cancelSource.IsCancellationRequested)
                    return true;
                var token = cancelSource.Token;
                var executionTask = executor.EnqueueTask(() => UpdateTeamPresence(team, reason, token, force), token);
                var result = await executionTask.ConfigureAwait(false);
                //if (groupStateChangeAborters.TryRemove(team.UserId, out var aborter))
                //    dispose(aborter.Value, team.UserId);
                return result;
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException || e is OperationCanceledException)
                {
                    Log($"Processing of presence state update for team {team.Name} was aborted because another state came in", 4);
                    return true;
                }
                else
                {
                    Log($"Something went wrong in {nameof(processTeamPresenceUpdate)}: {e.Message}", 2);
                    //if (userStateChangeAborters.TryRemove(team.UserId, out var aborter))
                    //    cancelAndDispose(oldSource, team.UserId, true);
                    return false;
                }
            }
            finally
            {
                if (oldSource != null)
                    dispose(oldSource, team.UserId);
                cancelAndDispose(cancelSource, team.UserId, true);
            }
        }

        internal async Task<bool> processUserPresenceUpdateAsync(User user, UserState state, UserState previousState, bool isInitialState = false)
        {
            var executor = getAsyncFifoExecutor(user.UserId);
            CancellationTokenSource oldSource = null, cancelSource = null;
            cancelSource = userStateChangeAborters.AddOrUpdate(user.UserId, _ => tokenFactory(user.UserId, true), (key, existingValue) =>
            {
                oldSource = existingValue.Value;
                return tokenFactory(key, true);
            }).Value;
            {
                if (oldSource != null)
                {
                    Log($"Cancelling execution of {nameof(processUserPresenceUpdateAsync)} for user {user.UserId} because there's a new state: {state}", 4);
                    cancel(oldSource, user.UserId);
                    //cancelAndDispose(oldSource, user.UserId);
                }
                Log($"Enqueuing presence state update for user {user.UserId} for source {cancelSource.GetHashCode()}", 5);
                try
                {
                    if (cancelSource.IsCancellationRequested)
                        return true;
                    var cancelToken = cancelSource.Token;
                    var executionTask = executor.EnqueueTask(() => processUserPresenceUpdateAsync(user, state, previousState,
                        cancelToken, isInitialState), cancelToken);
                    var result = await executionTask.ConfigureAwait(false);
                    //if (userStateChangeAborters.TryRemove(user.UserId, out var aborter))
                    //    dispose(aborter.Value, user.UserId);
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
                        Log($"Something went wrong in {nameof(processUserPresenceUpdateAsync)}: {e.Message}", 2);
                        //if (userStateChangeAborters.TryRemove(user.UserId, out var aborter))
                        //    cancelAndDispose(aborter.Value, user.UserId, true);
                        return false;
                    }
                }
                finally
                {
                    if (oldSource != null)
                        dispose(oldSource, user.UserId);
                    cancelAndDispose(cancelSource, user.UserId, true);
                }
            }
        }

        private async Task<bool> processUserPresenceUpdateAsync(User user, UserState state, UserState previousState,
            CancellationToken token, bool isInitialState = false)
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
                return false;
            await Task.Delay(operationDuration, token).ConfigureAwait(false);
            Log($"Successfully processed state change of {user.UserId} from {previousState} to {state}", 4);
            return true;
        }

        internal async Task<bool> UpdateTeamPresence(Team team, string reason, CancellationToken token, bool force = false)
        {
            Log($"Update of team presence state for team {team.Name} is executing. reason: {reason}", 5);
            UserState newStatus = GenerateTeamPresence(team);
            Log($"Updating team presence for team {team.Name}, reason {reason}, new status {newStatus}", 5);
            return await updateTeamPresence(newStatus, team, reason, token, force).ConfigureAwait(false);
        }

        private async Task<bool> updateTeamPresence(UserState status, Team team, string reason, CancellationToken token, bool force = false)
        {
            if (token.IsCancellationRequested)
            {
                Log($"aborting updatePresence for team {team.Name},trigger: {reason} because cancellation was requested at position 1", 4);
                return true;
            }
            if (groupPresenceStates.TryGetValue(team.UserId, out var existingStatus))
            {
                if (existingStatus == status)
                {
                    if (!force)
                    {
                        Log($"Presence of team {team.Name} is already set to {status}, trigger: {reason}", 4);
                        return true;
                    }
                    else
                        Log($"Presence of team {team.Name} is already set to {status}, forcing presence update, trigger: {reason}", 4);
                }
            }
            if (token.IsCancellationRequested)
            {
                Log($"aborting updatePresence for team {team.Name},trigger: {reason} because cancellation was requested at position 2", 4);
                return true;
            }
            Log($"going to set presence of team {team.Name} to {status}, trigger: {reason}", 5);
            if (existingStatus == UserState.DoNotDisturb && status == UserState.Available)
                await Task.Delay(operationDuration, token).ConfigureAwait(false);
            if (token.IsCancellationRequested)
            {
                Log($"aborting updatePresence for team {team.Name},trigger: {reason} because cancellation was requested at position 3", 4);
                return false;
            }
            await Task.Delay(operationDuration, token).ConfigureAwait(false);
            Log($"Successfully set presence state of team {team.Name} to {status} due to {reason}", 4);
            groupPresenceStates.AddOrUpdate(team.UserId, status, (key, existingValue) => status);
            return true;
        }

        private UserState GenerateTeamPresence(Team team)
        {
            var states = new List<UserState>();
            if (team.Members != null)
            {
                foreach (var member in team.Members)
                {
                    var usr = users.FirstOrDefault(u => u.UserId == member);
                    if (usr != null)
                    {
                        if (userPresenceStates.TryGetValue(usr.UserId, out var state))
                            states.Add(state);
                    }
                }
            }
            if (states.Any(x => x == UserState.Available))
                return UserState.Available;
            if (!states.Any(x => x == UserState.Available) && states.Any(x => x == UserState.OnThePhone))
                return UserState.DoNotDisturb;
            if (states.Any(x => x != UserState.Offline))
                return UserState.Away;
            return UserState.Offline;
        }

        private IExecutableAsyncFifoQueue<bool> getAsyncFifoExecutor(string presenceIdentifier)
        {
            return groupStateChangeExecutors.GetOrAdd(presenceIdentifier, _ => getNewExecutor()).Value;
        }

        private Lazy<IExecutableAsyncFifoQueue<bool>> getNewExecutor()
        {
            return new Lazy<IExecutableAsyncFifoQueue<bool>>(new AsyncCollectionAbortableFifoQueue<bool>(serverShutDownSource.Token));
            //return new ExecutableAsyncFifoQueue3<bool>(serverShutDownSource.Token);
            //return new ModernExecutableAsyncFifoQueue<bool>(serverShutDownSource.Token);
        }

        private Lazy<CancellationTokenSource> tokenFactory(string userId, bool isUser)
        {
            SemaphoreSlim myLock = null;
            if (isUser)
                myLock = userLocks.GetOrAdd(userId, _ => lockFactory()).Value;
            else
                myLock = groupLocks.GetOrAdd(userId, _ => lockFactory()).Value;
            try
            {
                myLock.Wait();
                var src = new CancellationTokenSource();
                Log($"Generating new TokenSource for {userId}: {src.GetHashCode()}", 4);
                return new Lazy<CancellationTokenSource>(src);
            }
            finally
            {
                try
                {
                    myLock.Release();
                }
                catch (Exception) { }
            }
        }

        private Lazy<SemaphoreSlim> lockFactory()
        {
            return new Lazy<SemaphoreSlim>(new SemaphoreSlim(1));
        }

        private void cancel(CancellationTokenSource source, string identifier)
        {
            try
            {
                if (!source.IsCancellationRequested)
                {
                    Log($"Cancelling token on source {source.GetHashCode()}, identifier: {identifier}", 6);
                    source.Cancel();
                }
            }
            catch (Exception) { }
        }

        private void cancelAndDispose(CancellationTokenSource source, string identifier, bool immediateDispose = false)
        {
            cancel(source, identifier);
            if (immediateDispose)
                dispose(source, identifier);
            else
                _ = delayedDispose(source, identifier);
        }

        private async Task delayedDispose(CancellationTokenSource src, string identifier)
        {
            try
            {
                await Task.Delay(20 * 1000).ConfigureAwait(false);
            }
            finally
            {
                dispose(src, identifier);
            }
        }

        private void dispose(CancellationTokenSource src, string identifier)
        {
            try
            {
                Log($"Disposing source {src.GetHashCode()} for {identifier}", 6);
                src.Dispose();
            }
            catch (ObjectDisposedException) { }
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

    }

    
}
