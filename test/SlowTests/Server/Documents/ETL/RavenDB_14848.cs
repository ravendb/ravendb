using Tests.Infrastructure;
using System;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_14848 : EtlTestBase
    {
        public RavenDB_14848(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrowOnInvalidConfigOnUpdate()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new Doc {Id = "doc-1", StrVal = "doc-1", StrVal2 = "doc-1"});
                    session.SaveChanges();
                }

                var putResult = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "test", TopologyDiscoveryUrls = dest.Urls, Database = dest.Database,
                }));

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms = {new Transformation() {Name = "allDocs", Collections = {"Docs"}, Script = @"loadToDocs({ StrVal: this.StrVal });",}}
                };

                var addResult = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));

                var configuration2 = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allDocs", Collections = {"Docs"}, Script = @"loadToDocs({ StrVal: this.StrVal, StrVal2: this.StrVal2 });", ApplyToAllDocuments = true,
                        }
                    }
                };

                var ex = Assert.Throws<RavenException>(() => src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(addResult.TaskId, configuration2)));

                Assert.Contains("Collections cannot be specified when ApplyToAllDocuments is set. Script name: 'allDocs'", ex.Message);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldResetEtl(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new Doc { Id = "doc-1", StrVal = "doc-1", StrVal2 = "doc-1" });
                    session.SaveChanges();
                }

                var putResult = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                }));

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms = { new Transformation() { Name = "allDocs", Collections = { "Docs" }, Script = @"loadToDocs({ StrVal: this.StrVal });", } }
                };

                var addResult = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var configuration2 = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allDocs", Collections = {"Docs"}, Script = @"loadToDocs({ StrVal: this.StrVal, StrVal2: this.StrVal2 });"
                        }
                    }
                };

                var updateResult = src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(addResult.TaskId, configuration2));

                etlDone.Reset();

                src.Maintenance.Send(new ResetEtlOperation("myConfiguration", "allDocs"));

                WaitForUserToContinueTheTest(src);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var doc1 = session.Load<Doc>("doc-1");
                    Assert.NotNull(doc1.StrVal2);
                }
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
            public string StrVal2 { get; set; }
        }
    }
}
