using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class ClientServer : RavenTest
	{
		public class Item
		{
		}

		[Fact]
		public void CanGetNotificationAboutDocumentPut()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Where(x=>x.Type==ChangeTypes.Put)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				var changeNotification = list.Take();

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);
			}
		}

		[Fact]
		public void CanGetNotificationAboutDocumentDelete()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Where(x => x.Type == ChangeTypes.Delete)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				var changeNotification = list.Take();

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Delete);
			}
		}

		[Fact]
		public void CanGetNotificationAboutDocumentIndexUpdate()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Where(x => x.Type == ChangeTypes.IndexUpdated)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				var changeNotification = list.Take();

				Assert.Equal("Raven/DocumentsByEntityName", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.IndexUpdated);
			}
		}
	}
}