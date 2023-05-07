using System;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_9403 : RavenTestBase
    {
        public RavenDB_9403(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Identifier_of_loaded_doc_should_not_be_created_using_cluster_identities(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", "loadToPeople(this);");

                var etlDone = Etl.WaitForEtlToComplete(src, (n, s) => s.LoadSuccesses > 0);

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
