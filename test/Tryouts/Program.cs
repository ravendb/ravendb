using System;
using System.Diagnostics;
using SlowTests.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();
            for (int i = 0; i < 10; i++)
            {

                var sp = Stopwatch.StartNew();
                using (var a = new CommandsTests())
                {
                    a.When_command_committed_CompletionTaskSource_is_notified().Wait();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }
}