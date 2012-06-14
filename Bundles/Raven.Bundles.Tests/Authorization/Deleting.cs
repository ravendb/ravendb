//-----------------------------------------------------------------------
// <copyright file="Deleting.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using Raven.Client.Connection;
using Raven.Client.Document;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;

using System;
using Raven.Bundles.Tests.Versioning;
using Xunit;

namespace Raven.Bundles.Tests.Authorization
{
	public class Deleting : AuthorizationTest
	{
		[Fact]
		public void WillAbortDeleteIfUserDoesNotHavePermissions()
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

				Assert.Throws<InvalidOperationException>(() => ((DocumentSession)s).DatabaseCommands.Delete(company.Id, null));
			}
		}

		[Fact]
		public void WillDeleteIfUserHavePermissions()
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
				company.Name = "Stampading Rhinos";
				s.Store(company);

				Assert.DoesNotThrow(() => store.DatabaseCommands.Delete(company.Id, null));
			}
		}
	}
}
