using System;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Employee = Orders.Employee;

namespace SlowTests.Issues
{
    public class RavenDB_24486 : RavenTestBase
    {
        public RavenDB_24486(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public void EtlWithEmptyPostfixShouldNotGenerateDuplicateSlashesInId()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var connectionStringName = "my-conn";

                Etl.AddEtl(src, new RavenEtlConfiguration
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = { "Employees" },
                            Script = "loadToNewEmployees({ Name: this.LastName });",
                            DocumentIdPostfix = ""
                        }
                    }
                }, new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = dest.Urls
                });

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new Employee { LastName = "Doe" });
                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    var docs = session.Advanced.LoadStartingWith<Employee>("employees/1-A/newEmployees/");
                    Assert.Equal(1, docs.Length);

                    var docId = session.Advanced.GetDocumentId(docs[0]);
                    Assert.DoesNotContain("//", docId);
                }
            }
        }
    }
}
