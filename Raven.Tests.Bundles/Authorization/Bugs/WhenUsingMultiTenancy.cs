//-----------------------------------------------------------------------
// <copyright file="WhenUsingMultiTenancy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using Raven.Client.Extensions;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class WhenUsingMultiTenancy : AuthorizationTest
	{
		[Fact]
		public void BugWhenSavingDocumentOnDatabase()
		{
			string database = "test_auth";
			store.DatabaseCommands.EnsureDatabaseExists(database);

			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession(database))
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

			using (var s = store.OpenSession(database))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

				Assert.NotNull(s.Load<Company>(company.Id));
			}
		}
	}
}