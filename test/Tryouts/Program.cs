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
            using (var test = new SlowTests.Tests.Faceted.Aggregation())
            {
                try
                {
                    test.CanCorrectlyAggregate_DateTimeDataType_WithRangeCounts_AndInOperator_BeforeOtherWhere();
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
