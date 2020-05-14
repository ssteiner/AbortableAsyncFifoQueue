using System.Collections.Generic;

namespace AsyncFifoQueueMemoryLeak.Models
{
    internal enum UserState { Available, Away, Busy, DoNotDisturb, Offline, OnThePhone }


    internal class User
    {
        internal string UserId { get; set; }

        internal List<Team> Groups { get; set; }

        internal UserState? CurrentStatus { get; set; }
    }

    internal class Team
    {
        internal string Name { get; set; }

        internal string UserId { get; set; }

        internal List<string> Members { get; set; }
    }
}
