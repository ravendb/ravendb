using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15825 : RavenTestBase
    {
        public RavenDB_15825(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                await new ContactsIndex().ExecuteAsync(store);
                using (var session = store.OpenSession())
                {
                    var random = new Random();
                    for (int id = 0; id < 10000; id++)
                    {
                        int companyId = id % 100;
                        var contact = new Contact
                        {
                            Id = $"contacts/{id}",
                            CompanyId = companyId,
                            IsActive = id % 2 == 0,
                            Tags = new string[] { Tags[id % Tags.Length] }
                        };

                        session.Store(contact);
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var res = Facet(session, 1, 3, out var stats);
                    Assert.NotEqual(-1, stats.DurationInMs);
                    Assert.Equal(3, res[nameof(Contact.CompanyId)].Values.Count);
                    Assert.Equal("28", res[nameof(Contact.CompanyId)].Values[0].Range);
                    Assert.Equal("38", res[nameof(Contact.CompanyId)].Values[1].Range);
                    Assert.Equal("48", res[nameof(Contact.CompanyId)].Values[2].Range);
                    var res2 = Facet(session, 2, 1, out var stats2);
                    Assert.NotEqual(-1, stats2.DurationInMs);
                    Assert.Equal(1, res2[nameof(Contact.CompanyId)].Values.Count);
                    Assert.Equal("38", res2[nameof(Contact.CompanyId)].Values[0].Range);
                    var res3 = Facet(session, 5, 5, out var stats3);
                    Assert.NotEqual(-1, stats3.DurationInMs);
                    Assert.Equal(5, res3[nameof(Contact.CompanyId)].Values.Count);
                    Assert.Equal("68", res3[nameof(Contact.CompanyId)].Values[0].Range);
                    Assert.Equal("78", res3[nameof(Contact.CompanyId)].Values[1].Range);
                    Assert.Equal("8", res3[nameof(Contact.CompanyId)].Values[2].Range);
                    Assert.Equal("88", res3[nameof(Contact.CompanyId)].Values[3].Range);
                    Assert.Equal("98", res3[nameof(Contact.CompanyId)].Values[4].Range);
                }
            }
        }

        private static Dictionary<string, FacetResult> Facet(IDocumentSession session, int skip, int take, out QueryStatistics stats)
        {
            Dictionary<string, FacetResult> result = session.Query<ContactsIndex.Result, ContactsIndex>()
                .Statistics(out stats)
                .OrderBy(c => c.CompanyId, OrderingType.AlphaNumeric)
                .Where(c => c.IsActive)
                .Where(c => c.Tags.Contains("apple"))
                .AggregateBy(builder => builder.ByField(c => c.CompanyId).WithOptions(new FacetOptions { Start = skip, PageSize = take }))
                .Execute();

            return result;
        }

        private static readonly string[] Tags = { "test", "label", "vip", "apple", "orange" };

        private class Contact
        {
            public string Id { get; set; }

            public int CompanyId { get; set; }

            public bool IsActive { get; set; }

            public string[] Tags { get; set; }
        }

        private class ContactsIndex : AbstractIndexCreationTask<Contact, ContactsIndex.Result>
        {
            public ContactsIndex()
            {
                Map = contacts => from contact in contacts
                                  select new Result
                                  {
                                      CompanyId = contact.CompanyId,
                                      Tags = contact.Tags,
                                      IsActive = contact.IsActive
                                  };
            }

            public class Result
            {
                public int CompanyId { get; set; }

                public bool IsActive { get; set; }

                public string[] Tags { get; set; }
            }
        }
    }
}
