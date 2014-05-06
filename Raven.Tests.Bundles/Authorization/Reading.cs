//-----------------------------------------------------------------------
// <copyright file="Reading.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System.Linq;

using Raven.Client.Exceptions;

using Xunit;

namespace Raven.Tests.Bundles.Authorization
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
			using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization());// deny everyone

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

				var readVetoException = Assert.Throws<ReadVetoException>(() => s.Load<Company>(company.Id));

				Assert.Equal(@"Document could not be read because of a read veto.
The read was vetoed by: Raven.Bundles.Authorization.Triggers.AuthorizationReadTrigger
Veto reason: Could not find any permissions for operation: Company/Bid on companies/1 for user Authorization/Users/Ayende.
No one may perform operation Company/Bid on companies/1
", readVetoException.Message);
			}
		}

		[Fact]
		public void CanReadDocumentWhichWasNotSecured()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
            using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				// by not specifying that, we say that anyone can read this
				//s.SetAuthorizationFor(company, new DocumentAuthorization());

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

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
            using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions =
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission
							{
								User = UserId,
								Allow = true,
								Operation = "Company/Bid"
							}
						}
				});

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

				Assert.NotNull(s.Load<Company>(company.Id));
			}
		}

		[Fact]
		public void DocumentWithoutPermissionWillBeFilteredOutSilently()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
            using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization());// deny everyone

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

                Assert.Equal(0, s.Advanced.DocumentQuery<Company>()
									.WaitForNonStaleResults()
				                	.ToList().Count);
			}
		}
	}
}
