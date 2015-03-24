using Raven.Tests.Bundles.Authorization;

namespace Raven.Bundles.Tests.Authorization.Bugs
{
    extern alias client;
    using client::Raven.Bundles.Authorization.Model;
    using client::Raven.Client.Authorization;
    using System.Linq;
    using Xunit;

    public class PrestonLazy : AuthorizationTest
    {
        [Fact]
        public void DocumentWithoutPermissionWillBeFilteredOutSilently()
        {
            var company = new Company
            {
                Name = "Hibernating Rhinos"
            };
            using (var s = store.OpenSession())
            {
                s.Store(new AuthorizationUser
                {
                    Id = UserId,
                    Name = "Ayende Rahien",
                });

                s.Store(company);

                s.SetAuthorizationFor(company, new DocumentAuthorization());// deny everyone

                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                s.SecureFor(UserId, "Company/Bid");

                Assert.Equal(0, s.Advanced.LuceneQuery<Company>()
                                    .WaitForNonStaleResults()
                                    //.ToList()
                                    .Lazily().Value
                                    .Count());
            }
        }

    }
}