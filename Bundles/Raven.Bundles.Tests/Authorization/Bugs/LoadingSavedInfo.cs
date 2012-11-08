//-----------------------------------------------------------------------
// <copyright file="LoadingSavedInfo.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System;
using System.Linq;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;
using Xunit;

namespace Raven.Bundles.Tests.Authorization.Bugs
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
													   Operation = "Company/Bid"
											   }
									   }
				});

				s.SaveChanges();
			}

			for (int i = 0; i < 15; i++)
			{
				using (var s = store.OpenSession())
				{
					s.SecureFor(UserId, "Company/Bid");

					var c = s.Load<Company>(company.Id);
					c.Name = "other " + i;

					s.SaveChanges();
				}

			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Bid");

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
													   Operation = "Company/Bid"
											   }
									   }
				});

				s.SaveChanges();
			}

			for (int i = 0; i < 15; i++)
			{
				using (var s = store.OpenSession())
				{
					s.SecureFor(UserId, "Company/Bid");

					var c = s.Query<Company>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(3))).First();
					c.Name = "other " + i;

					s.SaveChanges();
				}

			}

			using (var s = store.OpenSession())
			{
				s.SecureFor(UserId, "Company/Bid");

				var load = s.Load<Company>(company.Id);
				Assert.NotNull(load);
				Assert.Equal("other 14", load.Name);
			}
		}
	}
}