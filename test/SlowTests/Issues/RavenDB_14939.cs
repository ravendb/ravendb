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
            string analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                ModifyDatabaseRecord = record => record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                {
                    { analyzerName, new AnalyzerDefinition
                    {
                        Name = analyzerName,
                        Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                    }}
                }
            }))
            {
                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }

            foreach (var key in AnalyzerCompilationCache.Instance.PerDatabaseCache.ForceEnumerateInThreadSafeManner())
                Assert.NotEqual(analyzerName, key.Key.ResourceName);
        }

        [Fact]
        public void CanUseCustomAnalyzer_Restart()
        {
            string analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                ModifyDatabaseRecord = record => record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                {
                    { analyzerName, new AnalyzerDefinition
                    {
                        Name = analyzerName,
                        Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                    }}
                },
                RunInMemory = false
            }))
            {
                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithOperations()
        {
            string analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName
            }))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                store.Maintenance.Send(new DeleteAnalyzerOperation(analyzerName));

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", errors[0].Errors[0].Error);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithConfiguration()
        {
            string analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                ModifyDatabaseRecord = record =>
                {
                    record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                    {
                        { analyzerName, new AnalyzerDefinition
                        {
                            Name = analyzerName,
                            Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                        }}
                    };

                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = analyzerName;
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
            string analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = analyzerName;
                }
            }))
            {
                store.ExecuteIndex(new MyIndex_WithoutAnalyzer());

                Fill(store);

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: @default", errors[0].Errors[0].Error);

                store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
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
