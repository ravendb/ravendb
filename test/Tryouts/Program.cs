using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using FastTests.Voron.Bugs;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Tryouts.Corax;
using Voron;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //new DuplicatePageUsage().ShouldNotHappen();
            Run().Wait();
        }

        private static async Task Run()
        {
            using (var corax = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                using (var indexer = corax.CreateIndexer())
                {
                    for (int a = 0; a < 10; a++)
                    {
                        int index = 0;
                        foreach (var line in File.ReadLines(@"C:\Users\Ayende\Downloads\places.txt"))
                        {
                            await indexer.NewEntry(new DynamicJsonValue
                            {
                                ["Location"] = line
                            }, "users/" + (++index));

                        }
                    }
                }

                using (var searcher = corax.CreateSearcher())
                {
                    var ids = searcher.Query("Name", "Oren Eini");
                    Console.WriteLine(ids.Length);
                    //Assert.Equal(new[] { "users/1" }, ids);
                }

                //using (var indexer = corax.CreateIndexer())
                //{
                //   indexer.Delete("users/1");
                //}

                using (var searcher = corax.CreateSearcher())
                {
                    var ids = searcher.Query("Name", "Oren Eini");
                    Assert.Empty(ids);
                }

            }
        }
    }
}
