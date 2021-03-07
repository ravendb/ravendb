using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Extensions;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Analysis;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14939 : RavenTestBase
    {
        public RavenDB_14939(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseCustomAnalyzer()
        {
            string databaseName;
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                {
                    { "MyAnalyzer", new AnalyzerDefinition
                    {
                        Name = "MyAnalyzer",
                        Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs")
                    }}
                }
            }))
            {
                databaseName = store.Database;

                store.ExecuteIndex(new MyIndex());

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }

            foreach (var key in AnalyzerCompilationCache.AnalyzersPerDatabaseCache.ForceEnumerateInThreadSafeManner())
                Assert.NotEqual(databaseName, key.Key.DatabaseName);
        }

        [Fact]
        public void CanUseCustomAnalyzer_Restart()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                {
                    { "MyAnalyzer", new AnalyzerDefinition
                    {
                        Name = "MyAnalyzer",
                        Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs")
                    }}
                },
                RunInMemory = false
            }))
            {
                store.ExecuteIndex(new MyIndex());

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex().IndexName));

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithOperations()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex()));
                Assert.Contains("Cannot find analyzer type 'MyAnalyzer' for field: Name", e.Message);

                store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = "MyAnalyzer",
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs")
                }));

                store.ExecuteIndex(new MyIndex());

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                store.Maintenance.Send(new DeleteAnalyzerOperation("MyAnalyzer"));

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex().IndexName));

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains("Cannot find analyzer type 'MyAnalyzer' for field: Name", errors[0].Errors[0].Error);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithConfiguration()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                    {
                        { "MyAnalyzer", new AnalyzerDefinition
                        {
                            Name = "MyAnalyzer",
                            Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs")
                        }}
                    };

                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "MyAnalyzer";
                }
            }))
            {
                store.ExecuteIndex(new MyIndex_WithoutAnalyzer());

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex_WithoutAnalyzer>(store);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithConfiguration_NoAnalyzer()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "MyAnalyzer";
                }
            }))
            {
                store.ExecuteIndex(new MyIndex_WithoutAnalyzer());

                Fill(store);

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains("Cannot find analyzer type 'MyAnalyzer' for field: @default", errors[0].Errors[0].Error);

                store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = "MyAnalyzer",
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs")
                }));

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex_WithoutAnalyzer().IndexName));

                AssertCount<MyIndex_WithoutAnalyzer>(store);
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

        private static void AssertCount<TIndex>(IDocumentStore store)
            where TIndex : AbstractIndexCreationTask, new()
        {
            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<Customer, TIndex>()
                    .Customize(x => x.NoCaching())
                    .Search(x => x.Name, "Rogério*");

                Assert.Equal(results.Count(), 4);
            }
        }

        private static string GetAnalyzer(string name)
        {
            using (var stream = GetDump(name))
            using (var reader = new StreamReader(stream))
                return reader.ReadToEnd();
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_14939).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<Customer>
        {
            public MyIndex()
            {
                Map = customers => from customer in customers
                                   select new
                                   {
                                       Name = customer.Name
                                   };

                Indexes.Add(x => x.Name, FieldIndexing.Search);
                Analyzers.Add(x => x.Name, "MyAnalyzer");
            }
        }

        private class MyIndex_WithoutAnalyzer : AbstractIndexCreationTask<Customer>
        {
            public MyIndex_WithoutAnalyzer()
            {
                Map = customers => from customer in customers
                                   select new
                                   {
                                       Name = customer.Name
                                   };

                Indexes.Add(x => x.Name, FieldIndexing.Search);
            }
        }
    }
}
