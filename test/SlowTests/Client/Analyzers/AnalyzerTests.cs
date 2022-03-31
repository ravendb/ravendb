using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Analyzers
{
    public class AnalyzerTests : ClusterTestBase
    {
        public AnalyzerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanPutAndDeleteAnalyzer()
        {
            string analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = _ => analyzerName
                   }))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                await store.Maintenance.SendAsync(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));


                await new MyIndex(analyzerName).ExecuteAsync(store);

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                await store.Maintenance.SendAsync(new DeleteAnalyzerOperation(analyzerName));

                await store.Maintenance.SendAsync(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", errors[0].Errors[0].Error);
            }
        }

        [Fact]
        public async Task CanPutAndDeleteAnalyzerForSharding()
        {
            string analyzerName = GetDatabaseName();

            using (var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName
            }))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                await store.Maintenance.SendAsync(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));


                await new MyIndex(analyzerName).ExecuteAsync(store);

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                await store.Maintenance.SendAsync(new DeleteAnalyzerOperation(analyzerName));

                var shardNames = ShardHelper.GetShardNames(analyzerName, 3);
                foreach (var shard in shardNames)
                {
                    var shardNumber = ShardHelper.GetShardNumber(shard);
                    await store.Maintenance.ForShard(shardNumber).SendAsync(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));
                }

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(3, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", errors[0].Errors[0].Error);
            }
        }

        private void AssertCount<TIndex>(IDocumentStore store)
            where TIndex : AbstractIndexCreationTask, new()
        {
            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<Customer, TIndex>()
                    .Customize(x => x.NoCaching())
                    .Search(x => x.Name, "Rogério*");

                Assert.Equal(4, results.Count());
            }
        }

        private static void Fill(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Customer() { Name = "Rogério" });
                session.Store(new Customer() { Name = "Rogerio" });
                session.Store(new Customer() { Name = "Paulo Rogerio" });
                session.Store(new Customer() { Name = "Paulo Rogério" });
                session.SaveChanges();
            }
        }

        private static string GetAnalyzer(string resourceName, string originalAnalyzerName, string analyzerName)
        {
            using (var stream = GetDump(resourceName))
            using (var reader = new StreamReader(stream))
            {
                var analyzerCode = reader.ReadToEnd();
                analyzerCode = analyzerCode.Replace(originalAnalyzerName, analyzerName);

                return analyzerCode;
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(AnalyzerTests).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<Customer>
        {
            public MyIndex() : this("MyAnalyzer")
            {
            }

            public MyIndex(string analyzerName)
            {
                Map = customers => from customer in customers
                                   select new
                                   {
                                       Name = customer.Name
                                   };

                Indexes.Add(x => x.Name, FieldIndexing.Search);
                Analyzers.Add(x => x.Name, analyzerName);
            }
        }
    }
}
