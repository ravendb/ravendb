//-----------------------------------------------------------------------
// <copyright file="Writing.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;
using System;
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
				s.SecureFor(UserId, "Company/Rename");
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
								Operation = "Company/Rename"
							}
						}
				});// deny everyone

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Rename");
				s.Load<Company>(company.Id).Name = "Stampading Rhinos";

				Assert.DoesNotThrow(s.SaveChanges);
			}
		}

		[Fact]
		public void WillWriteIfUserHavePermissions_CaseInsensitive()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = UserId.ToUpper(),
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
								User = UserId.ToUpper(),
								Operation = "Company/Rename"
							}
						}
				});// deny everyone

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId.ToLower(), "Company/Rename");
				s.Load<Company>(company.Id).Name = "Stampading Rhinos";

				Assert.DoesNotThrow(s.SaveChanges);
			}
		}
	}
}
