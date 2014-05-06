//-----------------------------------------------------------------------
// <copyright file="LoadingSavedInfo.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System;
using System.Linq;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class LoadingSavedInfo : AuthorizationTest
	{
		[Fact]
		public void BugWhenSavingDocumentWithPreviousAuthorization()
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

			for (int i = 0; i < 15; i++)
			{
                using (var s = store.OpenSession(DatabaseName))
				{
					client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

					var c = s.Load<Company>(company.Id);
					c.Name = "other " + i;

					s.SaveChanges();
				}

			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

				var load = s.Load<Company>(company.Id);
				Assert.NotNull(load);
				Assert.Equal("other 14", load.Name);
			}
		}

		[Fact]
		public void BugWhenSavingDocumentWithPreviousAuthorization_WithQuery()
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

			for (int i = 0; i < 15; i++)
			{
                using (var s = store.OpenSession(DatabaseName))
				{
					client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

					var c = s.Query<Company>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(3))).First();
					c.Name = "other " + i;

					s.SaveChanges();
				}

			}

            using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

				var load = s.Load<Company>(company.Id);
				Assert.NotNull(load);
				Assert.Equal("other 14", load.Name);
			}
		}
	}
}