using System.Linq;
using Raven.Bundles.Authorization.Model;
using Raven.Bundles.Tests.Versioning;
using Raven.Client.Authorization;
using Raven.Client.Exceptions;
using Xunit;
using Raven.Client;

namespace Raven.Bundles.Tests.Authorization
{
	public class Reading : AuthorizationTest
	{
		[Fact]
		public void CannotReadDocumentWithoutPermissionToIt()
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
				s.SecureFor(UserId, "/Company/Bid");

				var readVetoException = Assert.Throws<ReadVetoException>(() => s.Load<Company>(company.Id));

				Assert.Equal(readVetoException.Message, @"Document could not be read because of a read veto.
The read was vetoed by: Raven.Bundles.Authorization.Triggers.AuthorizationReadTrigger
Veto reason: Could not find any permissions for operation: /Company/Bid on companies/1
");
			}
		}

		[Fact]
		public void CanReadDocumentWhichWasNotSecured()
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

				// by not specifying that, we say that anyone can read this
				//s.SetAuthorizationFor(company, new DocumentAuthorization());

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "/Company/Bid");

				Assert.NotNull(s.Load<Company>(company.Id));
			}
		}

		[Fact]
		public void CanReadDocumentWhichUserHavePermissionsTo()
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

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Permissions =
						{
							new DocumentPermission
							{
								User = UserId,
								Allow = true,
								Operation = "/Company/Bid"
							}
						}
				});

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "/Company/Bid");

				Assert.NotNull(s.Load<Company>(company.Id));
			}
		}

		[Fact]
		public void DocumentWithoutPermissionWillBeFilteredOutSiltently()
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
				s.SecureFor(UserId, "/Company/Bid");

                Assert.Equal(0, s.Advanced.LuceneQuery<Company>()
									.WaitForNonStaleResults()
				                	.ToList().Count);
			}
		}
	}
}