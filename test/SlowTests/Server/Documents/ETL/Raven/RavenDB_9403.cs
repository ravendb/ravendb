using System;
using FastTests.Server.JavaScript;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_9403 : EtlTestBase
    {
        public RavenDB_9403(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Identifier_of_loaded_doc_should_not_be_created_using_cluster_identities(Options options)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, "Users", "loadToPeople(this);");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    });

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var person = session.Load<Person>("users/1-A/people/0000000000000000001-A");

                    Assert.NotNull(person);
                    Assert.Equal("Joe Doe", person.Name);
                }
            }
        }
    }
}
