using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            
            Parallel.For(0, 25, async (i) =>
            {
                Console.WriteLine(i);
                using (var f = new SlowTests.Tests.Linq.WhereClause())
                {
                    await f.CanUnderstandSimpleContainsInExpression2();
                }
                Console.WriteLine(-i);
            });

        }
    }
}

