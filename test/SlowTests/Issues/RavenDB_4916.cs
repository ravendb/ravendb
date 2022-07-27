using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4916 : RavenTestBase
    {
        public RavenDB_4916(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new
                    {
                        Amount = 12.34m
                    });
                    session.Store(new
                    {
                        Amount = 11
                    });
                    session.Store(new
                    {
                        Amount = 13.23
                    });

                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "test",
                    Maps = { "from doc in docs select new { doc.Amount }" }
                }));

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var moneys = session.Advanced.DocumentQuery<dynamic>("test")
                        .WhereGreaterThanOrEqual("Amount", 10)
                        .ToArray();

                    Assert.Equal(3, moneys.Length);

                    moneys = session.Advanced.DocumentQuery<dynamic>("test")
                        .WhereGreaterThanOrEqual("Amount", 10.0)
                        .ToArray();

                    Assert.Equal(3, moneys.Length);

                    moneys = session.Advanced.DocumentQuery<dynamic>("test")
                        .WhereGreaterThanOrEqual("Amount", 10m)
                        .ToArray();

                    Assert.Equal(3, moneys.Length);
                }
            }
        }
    }
}
