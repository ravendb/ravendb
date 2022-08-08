using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.ServerWide.Operations.Analyzers;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16328_Analyzers : RavenTestBase
    {
        public RavenDB_16328_Analyzers(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseCustomAnalyzer(Options options)
        {
            var analyzerName = GetDatabaseName();
            options.ModifyDatabaseName = _ => analyzerName;

            using (var store = GetDocumentStore(options))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                store.Maintenance.Server.Send(new DeleteServerWideAnalyzerOperation(analyzerName));

                var resetIndex = store.Maintenance.ForTesting(() => new ResetIndexOperation(new MyIndex(analyzerName).IndexName));
                resetIndex.ExecuteOnAll();

                var errors = Indexes.WaitForIndexingErrors(store);

                var expectedNumberOfErrors = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3;

                Assert.Equal(expectedNumberOfErrors, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", errors[0].Errors[0].Error);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanOverrideCustomAnalyzer(Options options)
        {
            var analyzerName = GetDatabaseName();
            options.ModifyDatabaseName = _ => analyzerName;

            using (var store = GetDocumentStore(options))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_16328.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                var resetIndex = store.Maintenance.ForTesting(() => new ResetIndexOperation(new MyIndex(analyzerName).IndexName));
                resetIndex.ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store, expectedCount: 3);

                store.Maintenance.Send(new DeleteAnalyzerOperation(analyzerName));

                resetIndex.ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                store.Maintenance.Server.Send(new DeleteServerWideAnalyzerOperation(analyzerName));

                resetIndex.ExecuteOnAll();

                var errors = Indexes.WaitForIndexingErrors(store);

                var expectedNumberOfErrors = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3;

                Assert.Equal(expectedNumberOfErrors, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", errors[0].Errors[0].Error);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzer_Restart()
        {
            var serverPath = NewDataPath();
            var databasePath = NewDataPath();

            IOExtensions.DeleteDirectory(serverPath);
            IOExtensions.DeleteDirectory(databasePath);

            var analyzerName = GetDatabaseName();

            using (var server = GetNewServer(new ServerCreationOptions
            {
                DataDirectory = serverPath,
                RunInMemory = false
            }))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                Path = databasePath,
                RunInMemory = false,
                Server = server,
                DeleteDatabaseOnDispose = false
            }))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }

            AnalyzerCompilationCache.Instance.RemoveServerWideItem(analyzerName);

            using (var server = GetNewServer(new ServerCreationOptions
            {
                DataDirectory = serverPath,
                RunInMemory = false,
                DeletePrevious = false
            }))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                Path = databasePath,
                RunInMemory = false,
                Server = server,
                CreateDatabase = false
            }))
            {
                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzer_Restart_Faulty()
        {
            var serverPath = NewDataPath();
            var databasePath = NewDataPath();

            IOExtensions.DeleteDirectory(serverPath);
            IOExtensions.DeleteDirectory(databasePath);

            var analyzerName = GetDatabaseName();

            using (var server = GetNewServer(new ServerCreationOptions
            {
                DataDirectory = serverPath,
                RunInMemory = false
            }))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                Path = databasePath,
                RunInMemory = false,
                Server = server,
                DeleteDatabaseOnDispose = false
            }))
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MyIndex(analyzerName)));
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: Name", e.Message);

                var analyzerCode = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName);

                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = analyzerCode
                }));

                store.ExecuteIndex(new MyIndex(analyzerName));

                Fill(store);

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                // skipping compilation on purpose
                using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var command = new PutServerWideAnalyzerCommand(
                        new AnalyzerDefinition { Name = analyzerName, Code = analyzerCode.Replace(analyzerName, "MyAnalyzer") },
                        RaftIdGenerator.NewId());

                    using (var json = context.ReadObject(command.ValueToJson(), command.Name))
                    {
                        ClusterStateMachine.PutValueDirectly(context, command.Name, json, 1);
                    }

                    tx.Commit();
                }
            }

            AnalyzerCompilationCache.Instance.RemoveServerWideItem(analyzerName);

            using (var server = GetNewServer(new ServerCreationOptions
            {
                DataDirectory = serverPath,
                RunInMemory = false,
                DeletePrevious = false
            }))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                Path = databasePath,
                RunInMemory = false,
                Server = server,
                CreateDatabase = false
            }))
            {
                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                AssertErrors(store);

                // can override with database analyzer
                store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);

                // can go back to server analyzer
                store.Maintenance.Send(new DeleteAnalyzerOperation(analyzerName));

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                AssertErrors(store);

                // can fix server analyzer
                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.Maintenance.Send(new ResetIndexOperation(new MyIndex(analyzerName).IndexName));

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex>(store);
            }

            void AssertErrors(IDocumentStore store)
            {
                var errors = Indexes.WaitForIndexingErrors(store);

                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains("is an implementation of a faulty analyzer", errors[0].Errors[0].Error);
                Assert.Contains("Could not find type", errors[0].Errors[0].Error);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithConfiguration()
        {
            var analyzerName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => analyzerName,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = analyzerName;
                }
            }))
            {
                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = analyzerName,
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", analyzerName)
                }));

                store.ExecuteIndex(new MyIndex_WithoutAnalyzer());

                Fill(store);

                Indexes.WaitForIndexing(store);

                AssertCount<MyIndex_WithoutAnalyzer>(store);
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithConfiguration_NoAnalyzer()
        {
            var analyzerName = GetDatabaseName();

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

                var errors = Indexes.WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains($"Cannot find analyzer type '{analyzerName}' for field: @default", errors[0].Errors[0].Error);

                store.Maintenance.Server.Send(new PutServerWideAnalyzersOperation(new AnalyzerDefinition
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
                session.Store(new Customer { Name = "Rogério" });
                session.Store(new Customer { Name = "Rogerio" });
                session.Store(new Customer { Name = "Paulo Rogerio" });
                session.Store(new Customer { Name = "Paulo Rogério" });
                session.Store(new Customer { Name = "Paulo Rogerio Secondado" });
                session.Store(new Customer { Name = "Paulo Rogério Secondado" });
                session.SaveChanges();
            }
        }

        private void AssertCount<TIndex>(IDocumentStore store, int expectedCount = 6)
            where TIndex : AbstractIndexCreationTask, new()
        {
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<Customer, TIndex>()
                    .Customize(x => x.NoCaching())
                    .Search(x => x.Name, "Rogério*");

                Assert.Equal(expectedCount, results.Count());
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
