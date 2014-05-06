using System;

using Raven.Client.Document;
using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
	public class Troy3 : UniqueConstraintsTest
	{
		public class Account
		{
			public string Id { get; set; }
			[UniqueConstraint]
			public string Email { get; set; }
			public string Password { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public DateTimeOffset Created { get; set; }
			public DateTimeOffset LastModified { get; set; }
		}

		[Fact]
		public void Saving_Document_With_Unique_Constraints_Throws_Object_Reference_Not_Set()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var account = new Account
					{
						Email = "test@test.com",
						FirstName = "User",
						LastName = "Testing",
						Password = "testing",
						Created = DateTimeOffset.Now,
						LastModified = DateTimeOffset.Now
					};
				var check = session.CheckForUniqueConstraints(account);
				if (check.ConstraintsAreFree())
				{
					session.Store(account);
					session.SaveChanges();
				}
				Assert.True(account.Id != String.Empty);
			}
		}

		[Fact]
		public void Saving_Document_With_Unique_Constraints_Throws_Object_Reference_Not_Set_Remote()
		{
			DocumentStore store = null;
			try
			{
				store = new DocumentStore
					{
						Url = "http://localhost:8079"
					};
				store.RegisterListener(new UniqueConstraintsStoreListener());
				store.Initialize();

				using (var session = store.OpenSession())
				{
					var account = new Account
					{
						Email = "test@test.com",
						FirstName = "User",
						LastName = "Testing",
						Password = "testing",
						Created = DateTimeOffset.Now,
						LastModified = DateTimeOffset.Now
					};
					var check = session.CheckForUniqueConstraints(account);
					if (check.ConstraintsAreFree())
					{
						session.Store(account);
						session.SaveChanges();
					}
					Assert.True(account.Id != String.Empty);
				}
			}
			finally
			{
				if (store != null) { store.Dispose(); }
			}
		}
	}
}