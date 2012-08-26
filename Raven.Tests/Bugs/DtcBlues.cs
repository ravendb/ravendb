using System.Transactions;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DtcBlues : RavenTest
	{
		[Fact]
		public void CanQueryDtcForUncommittedItem()
		{
			using(var store = NewDocumentStore())
			{
				using(var tx = new TransactionScope())
				{
					System.Transactions.Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
					                                                      new ManyDocumentsViaDTC.DummyEnlistmentNotification(),
					                                                      EnlistmentOptions.None);

					using(var session = store.OpenSession())
					{
						session.Store(new User());
						session.SaveChanges();
					}


					tx.Complete();
				}
				using (var session = store.OpenSession())
				{
					session.Advanced.AllowNonAuthoritativeInformation = false;
					var user = session.Load<User>("users/1");
					Assert.NotNull(user);
				}
			}
		}
		
	}

	public class DtcBluesRemote : RemoteClientTest
	{
		[Fact]
		public void CanQueryDtcForUncommittedItem()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				for (int i = 0; i < 150; i++)
				{
					string id;
					using (var tx = new TransactionScope())
					{
						System.Transactions.Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
																			  new ManyDocumentsViaDTC.DummyEnlistmentNotification(),
																			  EnlistmentOptions.None);

						using (var session = store.OpenSession())
						{
							var entity = new User();
							session.Store(entity);
							session.SaveChanges();
							id = entity.Id;
						}


						tx.Complete();
					}
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						var user = session.Load<User>(id);
						Assert.NotNull(user);
					}
				}
			}
		}

	}

	public class DtcBluesRemoteAndTouchingTheDisk : RemoteClientTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.RunInMemory = false;
		}

		[Fact]
		public void CanQueryDtcForUncommittedItem()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				for (int i = 0; i < 150; i++)
				{
					string id;
					using (var tx = new TransactionScope())
					{
						System.Transactions.Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
																			  new ManyDocumentsViaDTC.DummyEnlistmentNotification(),
																			  EnlistmentOptions.None);

						using (var session = store.OpenSession())
						{
							var entity = new User();
							session.Store(entity);
							session.SaveChanges();
							id = entity.Id;
						}


						tx.Complete();
					}
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						var user = session.Load<User>(id);
						Assert.NotNull(user);
					}
				}
			}
		}

	}
}
