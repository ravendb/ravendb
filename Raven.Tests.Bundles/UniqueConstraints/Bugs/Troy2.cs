// //-----------------------------------------------------------------------
// // <copyright file="Troy2.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;

using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
	public class Troy2 : UniqueConstraintsTest
	{
		public class Account
		{
			public Guid Id { get; set; }
			[UniqueConstraint]
			public string Username { get; set; }
			[UniqueConstraint]
			public string Email { get; set; }
			public string Password { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public DateTimeOffset Created { get; set; }
			public DateTimeOffset LastModified { get; set; }
		}

		[Fact]
		public void Editing_Document_With_Unique_Constraints_SaveChanges_Vetos_Put()
		{
			DocumentStore.Conventions.TransformTypeTagNameToDocumentKeyPrefix = PreserveTypeTagNameToDocumentKeyPrefix;

			var accountId = Guid.Empty;

			using (var session = DocumentStore.OpenSession())
			{
				var account = SeedAccount();
				var check = session.CheckForUniqueConstraints(account);
				if (check.ConstraintsAreFree())
				{
					session.Store(account);
					session.SaveChanges();
					accountId = account.Id;
				}
				Assert.NotEqual(Guid.Empty, account.Id);
			}

			// I would think on an edit, you would not need to CheckForUniqueConstraints 
			using (var session = DocumentStore.OpenSession())
			{
				var account = session.Load<Account>(accountId);
				account.FirstName = "ChangedName";
				session.SaveChanges();
				Assert.Equal("ChangedName", account.FirstName);
			}

			// Just for giggles, performing a CheckForUniqueConstraints which fails obviously
			using (var session = DocumentStore.OpenSession())
			{
				var account = session.Load<Account>(accountId);
				account.FirstName = "ChangedName";
				var check = session.CheckForUniqueConstraints(account);
				if (check.ConstraintsAreFree())
				{
					session.SaveChanges();
					Assert.Equal("ChangedName", account.FirstName);
				}
			}
		}


		private static string PreserveTypeTagNameToDocumentKeyPrefix(string typeTagName)
		{
			return typeTagName;
		}

		private static Account SeedAccount()
		{
			var uniquePostFix = Guid.NewGuid().ToString().Replace("-", "");
			return new Account
			{
				Username = "User" + uniquePostFix,
				Email = "test" + uniquePostFix + "@test.com",
				FirstName = "User",
				LastName = "Testing",
				Password = "testing",
				Created = DateTimeOffset.Now,
				LastModified = DateTimeOffset.Now
			};
		}
	}
}