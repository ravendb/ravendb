using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Bundles.Authorization;

namespace Raven.Bundles.Tests.Authorization.Bugs
{
    extern alias client;
    using client::Raven.Bundles.Authorization.Model;
    using client::Raven.Client.Authorization;
    using System.Linq;
    using Xunit;

    public class PrestonStreaming : AuthorizationTest
    {
        [Fact]
        public void DocumentWithoutPermissionWillBeFilteredOutSilentlyWithStreaming()
        {
            new CompanyIndex().Execute(store);
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
            WaitForIndexing(store);
            using (var s = store.OpenSession())
            {
                s.SecureFor(UserId, "Company/Bid");
                var results = QueryExtensions.StreamAllFrom(s.Advanced.LuceneQuery<Company, CompanyIndex>(), s);

                Assert.Equal(0, results.Count());
            }
        }


    }

    public class Company
    {
        public string Name { get; set; }
    }

    public static class QueryExtensions
    {
        public static IEnumerable<TEntity> StreamAllFrom<TEntity>(IDocumentQuery<TEntity> query, IDocumentSession session)
        {
            using (var enumerator = session.Advanced.Stream(query))
            {
                while (enumerator.MoveNext())
                {
                    yield return enumerator.Current.Document;
                }
            }
        }
    }

    public class CompanyIndex : AbstractIndexCreationTask<Company>
    {
        public CompanyIndex()
        {
            Map = companies =>
                from company in companies
                select new { };
        }
    }

}