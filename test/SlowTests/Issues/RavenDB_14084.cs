using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14084 : RavenTestBase
    {
        public RavenDB_14084(ITestOutputHelper output) : base(output)
        {
        }

        private class Companies_ByUnknown : AbstractIndexCreationTask<Company>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Companies/ByUnknown",
                    Maps = { "from c in docs.Companies select new { Unknown = c.Unknown };" },
                };
            }
        }

        private class Companies_ByUnknown_WithIndexMissingFieldsAsNull : AbstractIndexCreationTask<Company>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Companies/ByUnknown/WithIndexMissingFieldsAsNull",
                    Maps = { "from c in docs.Companies select new { Unknown = c.Unknown };" },
                    Configuration =
                    {
                        { RavenConfiguration.GetKey(x => x.Indexing.IndexMissingFieldsAsNull), "true" }
                    }
                };
            }
        }

        [Fact]
        public void CanIndexMissingFieldsAsNull_Auto()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.IndexMissingFieldsAsNull)] = "true"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals("Unknown", (object)null)
                        .ToList();

                    Assert.Equal(0, companies.Count);
                }
            }
        }

        [Fact]
        public void CanIndexMissingFieldsAsNull_Static()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByUnknown().Execute(store);
                new Companies_ByUnknown_WithIndexMissingFieldsAsNull().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company, Companies_ByUnknown>()
                        .WhereEquals("Unknown", (object)null)
                        .ToList();

                    Assert.Equal(0, companies.Count);
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company, Companies_ByUnknown_WithIndexMissingFieldsAsNull>()
                        .WhereEquals("Unknown", (object)null)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                }
            }
        }
    }
}
