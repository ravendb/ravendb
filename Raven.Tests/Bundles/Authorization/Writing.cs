//-----------------------------------------------------------------------
// <copyright file="Writing.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System;

using Xunit;

namespace Raven.Tests.Bundles.Authorization
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
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Rename");
				company.Name = "Stampeding Rhinos";
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
								Allow = true,
								User = UserId,
								Operation = "Company/Rename"
							}
						}
				});// deny everyone

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Rename");
				s.Load<Company>(company.Id).Name = "Stampeding Rhinos";

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
            using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId.ToUpper(),
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions =
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission
							{
								Allow = true,
								User = UserId.ToUpper(),
								Operation = "Company/Rename"
							}
						}
				});// deny everyone

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId.ToLower(), "Company/Rename");
				s.Load<Company>(company.Id).Name = "Stampeding Rhinos";

				Assert.DoesNotThrow(s.SaveChanges);
			}
		}
	}
}
