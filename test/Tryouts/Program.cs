using System;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Smuggler;
using SlowTests.Core.AdminConsole;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Server.Replication;

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
                    using (var test = new SlowTests.Tests.Linq.CanCallLastOnArray())
                    {
                        try
                        {
                            test.WillSupportLast();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            Console.WriteLine("-------------");
                            throw;
                        }
                    }
                });
            }
        }
    }
}
