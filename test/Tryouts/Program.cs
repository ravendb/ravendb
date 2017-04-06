using System;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.Replication;
using SlowTests.Server.Rachis;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                Parallel.For(0, 10, _ =>
                {
                    using (var a = new CommandsTests())
                    {
                        a.Command_not_committed_after_timeout_CompletionTaskSource_is_notified().Wait();
                    }
                });
            }
        }
    }
}