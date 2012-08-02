extern alias client;
extern alias database;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;


using System.Collections.Generic;
using Raven.Bundles.Tests.Versioning;

using Raven.Client.Exceptions;
using Xunit;
using System.Linq;

namespace Raven.Bundles.Tests.Authorization.Bugs
{
	public class WithChangingOfUser : AuthorizationTest
	{
		[Fact]
		public void BugWhenUpdatingUserRolesLoad()
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
								Role = "Admins",
								Allow = true,
								Operation = "Company/Bid"
							}
						}
				});

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Bid");

				Assert.Throws<ReadVetoException>(() => s.Load<Company>(company.Id));
			}

			using (var s = store.OpenSession())
			{
				var user = s.Load<AuthorizationUser>(UserId);
				user.Roles = new List<string> {"Admins"};
				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Bid");

				s.Load<Company>(company.Id);
			}
		}

		[Fact]
		public void BugWhenUpdatingUserRolesQuery()
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
								Role = "Admins",
								Allow = true,
								Operation = "Company/Bid"
							}
						}
				});

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Bid");

				Assert.Empty(s.Query<Company>().ToArray());
			}

			using (var s = store.OpenSession())
			{
				var user = s.Load<AuthorizationUser>(UserId);
				user.Roles = new List<string> { "Admins" };
				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Bid");

				Assert.NotEmpty(s.Query<Company>().ToArray());
		
			}
		}
	}
}