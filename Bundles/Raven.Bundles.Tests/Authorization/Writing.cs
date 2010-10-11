using System;
using Raven.Bundles.Authorization.Model;
using Raven.Bundles.Tests.Versioning;
using Raven.Client.Authorization;
using Xunit;

namespace Raven.Bundles.Tests.Authorization
{
	public class Writing : AuthorizationTest
	{
		[Fact]
		public void WillAbortWriteIfUserDoesNotHavePermissions()
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
				s.SecureFor(UserId, "/Company/Rename");
				company.Name = "Stampading Rhinos";
				s.Store(company);

				Assert.Throws<InvalidOperationException>(() => s.SaveChanges());
			}
		}

		[Fact]
		public void WillWriteIfUserHavePermissions()
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
								Allow = true,
								User = UserId,
								Operation = "/Company/Rename"
							}
						}
				});// deny everyone

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "/Company/Rename");
				company.Name = "Stampading Rhinos";
				s.Store(company);

				Assert.DoesNotThrow(s.SaveChanges);
			}
		}
	}
}