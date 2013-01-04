using System.Transactions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class PatchingWithDTC : RavenTest
	{
		public class Item
		{
			public string Name;
		}

		[Fact]
		public void ShouldWork()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item{Name = "milk"});
					session.SaveChanges();
				}

				using(var tx = new TransactionScope())
				using (var session = store.OpenSession())
				{
					session.Advanced.Defer(new PatchCommandData
					{
						Key = "items/1",
						Patches = new PatchRequest[]
						{
							new PatchRequest
							{
								Type = PatchCommandType.Set,
								Name = "Name",
								Value = "Bread"
							}, 
					}
					});
					session.SaveChanges();
					tx.Complete();	
				}
				using (var session = store.OpenSession())
				{
					session.Advanced.AllowNonAuthoritativeInformation = false;
					Assert.Equal("Bread", session.Load<Item>(1).Name);
				}
			}
		}
	}
}