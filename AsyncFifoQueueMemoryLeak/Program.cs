using System;

namespace AsyncFifoQueueMemoryLeak
{
    class Program
    {
        static void Main(string[] args)
        {
            var tester = new Producer();
            Console.WriteLine("Test successfully started, type exit to stop");
            string str;
            do
            {
                str = Console.ReadLine();
                if (str == "start")
                    tester.StartTests();
                else if (str == "stop")
                    tester.StopTests();
            }
            while (str != "exit");
            tester.Shutdown();
        }
    }
}
