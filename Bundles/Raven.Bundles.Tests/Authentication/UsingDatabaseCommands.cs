using System;
using System.IO;
using System.Net;
using System.Transactions;
using Raven.Bundles.Authentication;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Bundles.Tests.Authentication
{
	public class UsingDatabaseCommands : AuthenticationTest
	{
		public UsingDatabaseCommands()
		{
			using (var session = embeddedStore.OpenSession())
			{
				session.Store(new AuthenticationUser
				{
					Name = "Ayende",
					Id = "Raven/Users/Ayende",
					AllowedDatabases = new[] { "*" }
				}.SetPassword("abc"));
				session.SaveChanges();
			}
			store.Credentials = new NetworkCredential("Ayende", "abc");

		}

		[Fact]
		public void CanSaveAttachment()
		{
			store.DatabaseCommands.PutAttachment("abc", null, new MemoryStream(), new RavenJObject());

			Assert.NotNull(store.DatabaseCommands.GetAttachment("abc"));
		}

		[Fact]
		public void CanUseDtc()
		{
			using(var tx = new TransactionScope())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Ayende",
					});
					session.SaveChanges();
				}
				tx.Complete();
			}

			using (var session = store.OpenSession())
			{
				session.Advanced.AllowNonAuthoritativeInformation = false;
				Assert.Equal("Ayende", session.Load<Item>("items/1").Name);
			}
		}


		[Fact]
		public void CanUseDtc_Promoted()
		{
			using (var tx = new TransactionScope())
			{
				Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id, new DummyEnlistmentNotification(), EnlistmentOptions.None);
				
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Ayende",
					});
					session.SaveChanges();
				}
				tx.Complete();
			}

			using (var session = embeddedStore.OpenSession())
			{
				session.Advanced.AllowNonAuthoritativeInformation = false;
				Assert.Equal("Ayende", session.Load<Item>("items/1").Name);
			}
		}

		[Fact]
		public void CanStoreRecoveryInfo()
		{
			store.DatabaseCommands.StoreRecoveryInformation(Guid.NewGuid(), Guid.NewGuid(),Guid.NewGuid().ToByteArray());
		}


		public class Item
		{
			public string Name { get; set; }
		}

		public class DummyEnlistmentNotification : IEnlistmentNotification
		{
			public static readonly Guid Id = Guid.NewGuid();

			public bool WasCommitted { get; set; }
			public void Prepare(PreparingEnlistment preparingEnlistment)
			{
				preparingEnlistment.Prepared();
			}

			public void Commit(Enlistment enlistment)
			{
				WasCommitted = true;
				enlistment.Done();
			}

			public void Rollback(Enlistment enlistment)
			{
				enlistment.Done();
			}

			public void InDoubt(Enlistment enlistment)
			{
				enlistment.Done();
			}
		}
	}
}