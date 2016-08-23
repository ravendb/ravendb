using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using SlowTests.Tests.Linq;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("First run");
            using (var f = new WhereClause())
            {
                f.CanUnderstandSimpleContainsInExpression2().Wait();
            }
            Console.WriteLine("starting");
            var tasks = new Task[100];
            for (int i = 0; i < tasks.Length; i++)
            {
                var copy = i;
                tasks[i] = Task.Run(async () =>
                {

                    Console.WriteLine(copy);
                    try
                    {
                        using (var f = new WhereClause())
                        {
                            await f.CanUnderstandSimpleContainsInExpression2();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                    Console.WriteLine(-copy);
                });
            }

            Task.WaitAll(tasks);

        }
    }
}

