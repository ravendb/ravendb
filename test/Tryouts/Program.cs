using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Server.Replication;
using SlowTests.Tests.Faceted;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new FacetsWithParameters())
                {
                    test.FacetShouldUseParameters_WithFacetBaseList();
                }
            }
        }
    }
}
