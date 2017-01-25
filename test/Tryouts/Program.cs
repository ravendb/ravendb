using System;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using SlowTests.Tests;
using SlowTests.Voron;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
               using (var a = new ReplicationResolveToDatabase())
                {
                    var res =Task.Run(a.ChangeDatabaseAndResolve);
                    var res2 =Task.Run(a.ResovleToDatabase);
                    var res3 =Task.Run(a.ResovleToDatabaseComplex);
                    var res4 =Task.Run(a.SetDatabaseResolverAtTwoNodes);
                    var res5 =Task.Run(a.UnsetDatabaseResolver);

                    Task.WaitAll(res, res2, res3, res4, res5);
                }
                Console.WriteLine($"{i} finished");
            }

        }
    }
}

