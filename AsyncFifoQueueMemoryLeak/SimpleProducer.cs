using AsyncFifoQueueMemoryLeak.Models;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncFifoQueueMemoryLeak
{
    internal class SimpleProducer
    {

        //variables defining the test
        readonly int nbOfusers = 10;
        readonly int minimumDelayBetweenTest = 1; // seconds
        readonly int maximumDelayBetweenTests = 6; // seconds
        readonly int operationDuration = 3; // number of seconds an operation takes in the tester

        private readonly Random rand;
        private List<User> users;
        private readonly SimpleLeakyConsumer consumer;

        protected CancellationTokenSource serverShutDownSource, testAbortSource;
        private CancellationToken internalToken = CancellationToken.None;

        internal SimpleProducer()
        {
            rand = new Random();
            testAbortSource = new CancellationTokenSource();
            serverShutDownSource = new CancellationTokenSource();
            generateTestObjects(nbOfusers, 0, false);
            consumer = new SimpleLeakyConsumer(serverShutDownSource, operationDuration);
        }

        internal void StartTests()
        {
            if (internalToken == CancellationToken.None || internalToken.IsCancellationRequested)
            {
                internalToken = testAbortSource.Token;
                foreach (var user in users)
                    _ = setNewUserPresence(internalToken, user);
            }
        }

        internal void StopTests()
        {
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
                catch (TaskCanceledException)
                {
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
            await consumer.ProcessStateChange(user.UserId).ConfigureAwait(false);
        }

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
        }

    }
}
