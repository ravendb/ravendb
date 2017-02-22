using System;
using System.Diagnostics;
using SlowTests.Server.Rachis;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var sw = new Stopwatch();
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new CommandsTests())
                {                
                    sw.Start();
                    test.When_command_committed_CompletionTaskSource_is_notified().Wait();
                }
            }

            
        }
    }
}