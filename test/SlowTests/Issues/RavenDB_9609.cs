using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9609 : RavenTestBase
    {
        public RavenDB_9609(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_put_projected_fields_into_select()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByContact().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company()
                    {
                        Phone = "1",
                        Contact = new Contact()
                        {
                            Name = "a"
                        }
                    });

                    session.SaveChanges();

                    var query = session.Query<Company, Companies_ByContact>()
                        .ProjectInto<ContactDetails>()
                        .Customize(x => x.WaitForNonStaleResults());

                    Assert.Equal("from index 'Companies/ByContact' select Name, Phone",  RavenTestHelper.GetIndexQuery(query).Query);

                    var results = query
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("1", results[0].Phone);
                    Assert.Equal("a", results[0].Name);
                }
            }
        }

        private class ContactDetails
        {
            public string Name { get; set; }

            public string Phone { get; set; }
        }

        private class Companies_ByContact : AbstractIndexCreationTask<Company>
        {
            public Companies_ByContact()
            {
                Map = companies => companies
                    .Select(x => new
                    {
                        Name = x.Contact.Name,
                        x.Phone
                    });

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
