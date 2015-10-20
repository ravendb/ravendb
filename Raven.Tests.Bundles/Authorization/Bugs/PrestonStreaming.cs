using System.Collections.Generic;
using Raven.Abstractions.Data;
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
        public void DocumentWithoutPermissionWillBeFilteredOutSilentlyWithQueryStreaming()
        {
            new CompanyIndex().Execute(store);
            var rhinosCompany = new Company
            {
                Name = "Hibernating Rhinos"
            };

						var secretCompany = new Company
						{
							Name = "Secret Co."
						};

						var authorizationUser = new AuthorizationUser
						{
							Id = UserId,
							Name = "Ayende Rahien",
						};

					  var operation = "Company/Bid"; 
					
            using (var s = store.OpenSession())
            {
                s.Store(authorizationUser);
								s.Store(rhinosCompany);
								s.Store(secretCompany);

								var documentAuthorization = new DocumentAuthorization();
								documentAuthorization.Permissions.Add(new DocumentPermission()
								{
									Allow = true,
									Operation = operation,
									User = UserId
								});

								s.SetAuthorizationFor(rhinosCompany, documentAuthorization); // allow Ayende Rahien
								s.SetAuthorizationFor(secretCompany, new DocumentAuthorization()); // deny everyone

                s.SaveChanges();
            }

            WaitForIndexing(store);

            using (var s = store.OpenSession())
            {
                s.SecureFor(UserId, operation);
				var expected = s.Advanced.LuceneQuery<Company, CompanyIndex>().ToList().Count();
				
				var results = QueryExtensions.StreamAllFrom(s.Advanced.LuceneQuery<Company, CompanyIndex>(), s);

	            Assert.Equal(expected, results.Count());
            }
        }

		[Fact]
		public void DocumentWithoutPermissionWillBeFilteredOutSilentlyWithStreaming()
		{
			var rhinosCompany = new Company
			{
				Name = "Hibernating Rhinos"
			};

			var secretCompany = new Company
			{
				Name = "Secret Co."
			};

			var authorizationUser = new AuthorizationUser
			{
				Id = UserId,
				Name = "Ayende Rahien",
			};

			var operation = "Company/Bid";

			using (var s = store.OpenSession())
			{
				s.Store(authorizationUser);
				s.Store(rhinosCompany);
				s.Store(secretCompany);

				var documentAuthorization = new DocumentAuthorization();
				documentAuthorization.Permissions.Add(new DocumentPermission()
				{
					Allow = true,
					Operation = operation,
					User = UserId
				});

				s.SetAuthorizationFor(rhinosCompany, documentAuthorization); // allow Ayende Rahien
				s.SetAuthorizationFor(secretCompany, new DocumentAuthorization()); // deny everyone

				s.SaveChanges();
			}


			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, operation);

				var results = 0;

				using (var it = s.Advanced.Stream<Company>("companies/"))
				{
					while (it.MoveNext())
					{
						results++;
					}
				}

				Assert.Equal(2, results);
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