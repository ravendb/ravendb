using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19533 : RavenTestBase
{
    public RavenDB_19533(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Querying)]
    public void CanGetStreamStatsCorrectlyWithRelatedDocsProjection()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 200; i++)
                {
                    var address = new Address { City = "London", Country = "UK", Street = "xxx" };
                    var user = new User();
                    
                    session.Store(address);
                    user.AddressId = address.Id;
                    
                    session.Store(user);
                }

                session.SaveChanges();
            }
            
            using (var session = store.OpenSession())
            {
                var queryDefinition = @"from Users as u 
                           load u.AddressId as a
                           select {
                               User: u,
                               Address: a
                           }";

                var query = session.Advanced.RawQuery<ProjectedClass>(queryDefinition);

                var reader = session.Advanced.Stream(query, out var stats);
                
                Assert.Equal(200, stats.TotalResults);
            }
        }
    }
    
    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string LastName { get; set; }
        public string AddressId { get; set; } 
    }

    private class Address
    {
        public string Id { get; set; }
        public string Country { get; set; }
        public string City { get; set; }
        public string Street { get; set; }
    }

    private class ProjectedClass
    {
        public User User { get; set; }
        public Address Address { get; set; }
    }
}
