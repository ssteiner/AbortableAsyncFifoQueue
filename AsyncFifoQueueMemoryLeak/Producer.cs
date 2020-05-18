using AsyncFifoQueueMemoryLeak.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncFifoQueueMemoryLeak
{

    /// <summary>
    /// class that generates the data for the tests and then triggers the tests
    /// </summary>
    internal class Producer
    {
        //variables defining the test
        readonly int nbOfusers = 10, nbOfTeams = 0;
        readonly int minimumDelayBetweenTest = 1; // seconds
        readonly int maximumDelayBetweenTests = 6; // seconds
        readonly int operationDuration = 3; // number of seconds an operation takes in the tester

        private readonly Random rand;
        private List<User> users;
        private List<Team> teams;
        //private readonly LeakyTestperformer tester;
        //private readonly SimpleLeakyConsumer tester2;
        private readonly LeakyConsumer tester2;

        protected CancellationTokenSource serverShutDownSource, testAbortSource;
        private CancellationToken internalToken = CancellationToken.None;

        internal Producer()
        {
            rand = new Random();
            testAbortSource = new CancellationTokenSource();
            serverShutDownSource = new CancellationTokenSource();
            generateTestObjects(nbOfusers, nbOfTeams, false);
            //tester2 = new SimpleLeakyConsumer(serverShutDownSource, operationDuration);
            tester2 = new LeakyConsumer(users, serverShutDownSource, operationDuration);
        }

        internal void StartTests()
        {
            if (internalToken == CancellationToken.None || internalToken.IsCancellationRequested)
            {
                //tester2.Log($"Starting test for {users.Count} users", 4);
                internalToken = testAbortSource.Token;
                //token.Register(systemShutdown);
                foreach (var user in users)
                    _ = setNewUserPresence(internalToken, user);
            }
        }

        internal void StopTests()
        {
            //tester2.Log($"Stopping test", 4);
            testAbortSource.Cancel();
            try
            {
                testAbortSource.Dispose();
            }
            catch (ObjectDisposedException) { }
            testAbortSource = new CancellationTokenSource();
        }

        internal void Shutdown()
        {
            serverShutDownSource.Cancel();
            //tester2.Stop();
        }

        private async Task setNewUserPresence(CancellationToken token, User user)
        {
            while (!token.IsCancellationRequested)
            {
                var nextInterval = rand.Next(minimumDelayBetweenTest, maximumDelayBetweenTests);
                try
                {
                    await Task.Delay(nextInterval * 1000, testAbortSource.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException e)
                {
                    tester2.Log($"testing tast for user {user.UserId} was aborted: {e.Message}", 4);
                    break;
                }
                //now randomly generate a new state and submit it to the tester class
                UserState? status;
                var nbStates = Enum.GetValues(typeof(UserState)).Length;
                if (user.CurrentStatus == null)
                {
                    var newInt = rand.Next(nbStates);
                    status = (UserState)newInt;
                }
                else
                {
                    do
                    {
                        var newInt = rand.Next(nbStates);
                        status = (UserState)newInt;
                    }
                    while (status == user.CurrentStatus);
                }
                _ = sendUserStatus(user, status.Value);
            }
        }

        private async Task sendUserStatus(User user, UserState status)
        {
            await tester2.ProcessStateChange(status, user).ConfigureAwait(false);
            //await tester.ProcessPresenceStateChange(status, user.UserId).ConfigureAwait(false);
        }

        /// <summary>
        /// generates object used for testing
        /// </summary>
        /// <param name="nbUsers"></param>
        /// <param name="nbTeams"></param>
        /// <param name="addAllUsersToTeams"></param>
        private void generateTestObjects(int nbUsers, int nbTeams, bool addAllUsersToTeams = false)
        {
            users = new List<User>();
            for (int i = 0; i < nbUsers; i++)
            {
                var usr = new User
                { 
                    UserId = $"User_{i}", 
                    Groups = new List<Team>()
                };
                users.Add(usr);
            }
            teams = new List<Team>();
            for (int i = 0; i < nbTeams; i++)
            {
                var team = new Team
                {
                    Name = $"Team {i}",
                    UserId = $"Team_{1}",
                    Members = new List<string>()
                };
                if (addAllUsersToTeams)
                    team.Members = users.Select(x => x.UserId).ToList();
                else // do it randomly
                {
                    int nbUsersInTeam = rand.Next(0, users.Count);
                    if (nbUsersInTeam > 0)
                    {
                        int nbUsersAdded = 0;
                        do
                        {
                            var userIndex = rand.Next(0, nbUsersInTeam);
                            var usr = users[userIndex];
                            if (!team.Members.Contains(usr.UserId))
                            {
                                team.Members.Add(usr.UserId);
                                nbUsersAdded++;
                            }
                        }
                        while (nbUsersAdded < nbUsersInTeam);
                    }
                }
                teams.Add(team);
            }
        }

    }
}
