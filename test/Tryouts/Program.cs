using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    Console.WriteLine(i);
                    using (var test = new RavenDB_11191())
                    {
                        test.NullableEnumWithSaveEnumAsInt();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

            }
        }
    }
}
