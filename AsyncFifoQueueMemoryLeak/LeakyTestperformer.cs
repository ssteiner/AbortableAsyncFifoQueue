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
    internal class LeakyTestperformer
    {

        private readonly ConcurrentDictionary<string, UserState> userPresenceStates;
        private readonly ConcurrentDictionary<string, UserState> groupPresenceStates;
        private ConcurrentDictionary<string, IExecutableAsyncFifoQueue<bool>> groupStateChangeExecutors;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> userStateChangeAborters;
        protected ConcurrentDictionary<string, CancellationTokenSource> groupStateChangeAborters;

        protected CancellationTokenSource serverShutDownSource;

        private int operationDuration = 1000, maximumNumberOfParallelOperations = 20;

        private readonly List<User> users;

        private readonly List<Team> teams;

        internal bool DisableTeams { get; set; }

        internal LeakyTestperformer(List<User> users, List<Team> teams, CancellationTokenSource serverShutDownSource, int operationDuration)
        {
            userPresenceStates = new ConcurrentDictionary<string, UserState>();
            groupPresenceStates = new ConcurrentDictionary<string, UserState>();
            userStateChangeAborters = new ConcurrentDictionary<string, CancellationTokenSource>();
            groupStateChangeAborters = new ConcurrentDictionary<string, CancellationTokenSource>();
            groupStateChangeExecutors = new ConcurrentDictionary<string, IExecutableAsyncFifoQueue<bool>>();

            this.serverShutDownSource = serverShutDownSource;
            this.teams = teams;
            this.users = users;
            this.operationDuration = operationDuration * 1000; // convert from seconds to milliseconds
        }

        internal void Stop()
        {
            serverShutDownSource.Cancel();
        }

        internal async Task ProcessPresenceStateChange(UserState state, string userId, bool forceSend = false)
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
                    foreach (var team in affectedTeams)
                    {
                        var task = processTeamPresenceUpdate(team, $"{$"user {entity} status changed to "}{state}");
                        processTasks.Add(task);
                    }
                    //Task updateTask = affectedTeams.ForEachAsync(maximumNumberOfParallelOperations, (team) =>
                    //{
                    //    return processTeamPresenceUpdate(team, $"{$"user {entity} status changed to "}{state}");
                    //});
                    //processTasks.Add(updateTask);
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
            CancellationTokenSource oldSource = null;
            using (var cancelSource = groupStateChangeAborters.AddOrUpdate(team.UserId, new CancellationTokenSource(), (key, existingValue) =>
            {
                oldSource = existingValue;
                return new CancellationTokenSource();
            }))
            {
                if (oldSource != null)
                {
                    Log($"Cancelling execution of {nameof(processTeamPresenceUpdate)} for team {team.Name} because there's a new call to {nameof(processTeamPresenceUpdate)}", 4);
                    cancelAndDispose(oldSource);
                    oldSource = null;
                }
                Log($"Enqueuing presence state update for team {team.Name}, reason: {reason}", 5);
                try
                {
                    var executionTask = executor.EnqueueTask(() => UpdateTeamPresence(team, reason, cancelSource.Token, force));
                    var result = await executionTask.ConfigureAwait(false);
                    groupStateChangeAborters.TryRemove(team.UserId, out var aborter);
                    return result;
                }
                catch (TaskCanceledException)
                {
                    Log($"Processing of presence state update for team {team.Name} was aborted because another state came in", 4);
                    return false;
                }
                catch (Exception e)
                {
                    Log($"Something went wrong in {nameof(processTeamPresenceUpdate)}: {e.Message}", 2);
                    userStateChangeAborters.TryRemove(team.UserId, out var aborter);
                    return false;
                }
            }
        }

        internal async Task<bool> processUserPresenceUpdateAsync(User user, UserState state, UserState previousState, bool isInitialState = false)
        {
            var executor = getAsyncFifoExecutor(user.UserId);
            CancellationTokenSource oldSource = null;
            using (var cancelSource = userStateChangeAborters.AddOrUpdate(user.UserId, new CancellationTokenSource(), (key, existingValue) =>
            {
                oldSource = existingValue;
                return new CancellationTokenSource();
            }))
            {
                if (oldSource != null)
                {
                    Log($"Cancelling execution of {nameof(processUserPresenceUpdateAsync)} for user {user.UserId} because there's a new state: {state}", 4);
                    cancelAndDispose(oldSource);
                    oldSource = null;
                }
                Log($"Enqueuing presence state update for user {user.UserId}", 5);
                try
                {
                    var executionTask = executor.EnqueueTask(() => processUserPresenceUpdateAsync(user, state, previousState,
                        cancelSource.Token, isInitialState));
                    if (cancelSource.Token.IsCancellationRequested)
                        return false;
                    var result = await executionTask.ConfigureAwait(false);
                    userStateChangeAborters.TryRemove(user.UserId, out var aborter);
                    return result;
                }
                catch (TaskCanceledException)
                {
                    Log($"Processing of presence state update for user {user.UserId} was aborted because another state came in", 4);
                    return true;
                    //all good here.. we aborted execution on purpose
                }
                catch (Exception e)
                {
                    Log($"Something went wrong in {nameof(processUserPresenceUpdateAsync)}: {e.Message}", 2);
                    userStateChangeAborters.TryRemove(user.UserId, out var aborter);
                    return false;
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
            return groupStateChangeExecutors.GetOrAdd(presenceIdentifier, getNewExecutor());
        }

        private IExecutableAsyncFifoQueue<bool> getNewExecutor()
        {
            //return new ExecutableAsyncFifoQueue<bool>();
            return new ExecutableAsyncFifoQueue3<bool>(serverShutDownSource.Token);
            //return new ModernExecutableAsyncFifoQueue<bool>(serverShutDownSource.Token);
        }

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

    }

    
}
