using System;
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
            using (var test = new AdminJsConsoleTests())
            {
                try
                {
                    test.CanConvertAllJsonTypesToString().Wait();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.WriteLine("-------------");
                    throw;
                }
            }
        }
    }
}
