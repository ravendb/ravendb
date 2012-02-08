using System;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CannotChangeId
	{
		public class Item
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void StandardConvention_ModifiedProperty()
		{
			using(var store = new EmbeddableDocumentStore{RunInMemory = true}.Initialize())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item = session.Load<Item>("items/1");
					item.Id = "items/2";
					item.Name = "abc";
					Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
				}
			}
		}

		[Fact]
		public void StandardConvention_ModifiedJustId()
		{
			using (var store = new EmbeddableDocumentStore { RunInMemory = true }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item = session.Load<Item>("items/1");
					item.Id = "items/2";
					Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
				}
			}
		}

		[Fact]
		public void NameConvention_ModifiedProperty()
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Conventions =
					{
						FindIdentityProperty = info => info.Name == "Name"
					}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item = session.Load<Item>("Ayende");
					item.Id = "items/2";
					item.Name = "abc";
					Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
				}
			}
		}

		[Fact]
		public void NameConvention_ModifiedJustId()
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Conventions =
				{
					FindIdentityProperty = info => info.Name == "Name"
				}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item = session.Load<Item>("Ayende");
					item.Name = "abc";
					Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
				}
			}
		}
	}
}