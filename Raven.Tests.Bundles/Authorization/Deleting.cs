//-----------------------------------------------------------------------
// <copyright file="Deleting.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System;

using Raven.Abstractions.Connection;
using Raven.Client.Document;

using Xunit;

namespace Raven.Tests.Bundles.Authorization
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

				var e = Assert.Throws<ErrorResponseException>(() => ((DocumentSession)s).DatabaseCommands.Delete(company.Id, null));

                Assert.Contains("OperationVetoedException", e.Message);
                Assert.Contains("Raven.Bundles.Authorization.Triggers.AuthorizationDeleteTrigger", e.Message);
                Assert.Contains("Could not find any permissions for operation: Company/Rename on companies/1 for user Authorization/Users/Ayende", e.Message);
                Assert.Contains("No one may perform operation Company/Rename on companies/1", e.Message);
			}
		}

		[Fact]
		public void WillDeleteIfUserHavePermissions()
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
				company.Name = "Stampeding Rhinos";
				s.Store(company);

				Assert.DoesNotThrow(() => store.DatabaseCommands.Delete(company.Id, null));
			}
		}
	}
}
