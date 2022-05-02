using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Commercial;
using Raven.Server.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18554 : RavenTestBase
    {
        public RavenDB_18554(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task QueriesShouldFailoverIfIndexIsCompacting()
        {
            int n = 500;
            int m = 5;

            using (var store = GetDocumentStore(new Options { RunInMemory = false }))
            {
                // Prepare Server For Test
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < n; i++)
                    {
                        Category c = new Category {Name = $"n{i}", Description = $"d{i}"};
                        await session.StoreAsync(c);
                    }
                    await session.SaveChangesAsync();
                }
                var indexName = await CreateIndex(store);
                string[] indexNames = {indexName};
                CompactSettings settings = new CompactSettings {DatabaseName = "QueriesShouldFailoverIfIndexIsCompacting_1", Documents = true, Indexes = indexNames};

                var cs = new CancellationTokenSource();
                var t1 = Task.Run(async () =>
                {
                     for (int i = 0; i < m; i++)
                     {
                         if (cs.IsCancellationRequested)
                             return;
                         var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(settings), cs.Token);
                         await operation.WaitForCompletionAsync();
                     }
                }, cs.Token);

                for (int i = 0; i < m; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var curId = $"categories/{i + 1}-A";
                        var l = await session.Query<Categoroies_Details.Entitiy, Categoroies_Details>()
                            .Where(x => x.Id == curId)
                            .ProjectInto<Categoroies_Details.Entitiy>()
                            .ToListAsync();

                        //Console.WriteLine("***" + (l.Count > 0 ? l[0] : "Empty List"));
                    }
                }

                cs.Cancel();
                await t1;
                //WaitForUserToContinueTheTest(store);
            }

        }

        async Task<string> CreateIndex(DocumentStore store)
        {
            //Create Index
            var index = new Categoroies_Details();
            index.Execute(store);

            //Wait for indexing
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<Categoroies_Details.Entitiy, Categoroies_Details>()
                    .Customize(c => c.WaitForNonStaleResults())
                    .Where(x => x.Id == "categories/1-A")
                    .Select(x => x.Id)
                    .ToListAsync();
            }
            return index.IndexName;

            // Wait for indexing
            //var indexName = index.IndexName;
            // DatabaseStatistics? stats = store.Maintenance.Send(new GetStatisticsOperation());
            // while (stats.Indexes.Where(x => x.Name == indexName).Where(x => x.IsStale == false).ToList().Count > 0)
            // {
            //     await Task.Delay(100);
            // }
            //

            // using (var session = store.OpenSession())
            // {
            //     QueryStatistics stats = null;
            //     bool firstIteration = true;
            //     do
            //     {
            //         if (firstIteration)
            //         {
            //             firstIteration = false;
            //         }
            //         else
            //         {
            //             await Task.Delay(100);
            //         }
            //         session.Query<Product>()
            //             .Statistics(out stats)
            //             .Where(x => x.Id == "aaa")
            //             .ToList();
            //     } while (stats!=null && stats.IsStale);
            // }
            //return indexName;
        }

        class Categoroies_Details : AbstractMultiMapIndexCreationTask<Categoroies_Details.Entitiy>
        {
            internal class Entitiy
            {
                public string Id { get; set; }
                public string Details { get; set; }

                public override string ToString()
                {
                    return $"Id=\"{Id}\", Details=\"{Details}\"";
                }
            }

            public Categoroies_Details()
            {
                AddMap<Category>(
                    categories =>
                        from c in categories
                        select new Entitiy
                        {
                            Id = c.Id,
                            Details = $"Id=\"{c.Id}\", Name=\"{c.Name}\", Description=\"{c.Description}\""
                        }
                );
                Store(x => x.Details, FieldStorage.Yes);
            }
        }
    }
}

