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

		public override void Dispose()
		{
			base.Dispose();
			GC.Collect(GC.MaxGeneration);
			GC.WaitForPendingFinalizers();
		}

		[Fact]
		public void CanGetNotificationAboutDocumentPut()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://ipv4.fiddler:8079",
				Conventions =
					{
						FailoverBehavior = FailoverBehavior.FailImmediately
					}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Load<object>("test-start");
				}
				var list = new BlockingCollection<DocumentChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.DocumentSubscription("items/1");
				observableWithTask.Task.Wait();
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(5)));

				Assert.Equal("items/1", documentChangeNotification.Name);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
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
				var list = new BlockingCollection<DocumentChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				taskObservable
					.DocumentSubscription("items/1")
					.Where(x => x.Type == DocumentChangeTypes.Delete)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				DocumentChangeNotification DocumentChangeNotification;
				Assert.True(list.TryTake(out DocumentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", DocumentChangeNotification.Name);
				Assert.Equal(DocumentChangeNotification.Type, DocumentChangeTypes.Delete);
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
				var list = new BlockingCollection<IndexChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				taskObservable
					.IndexSubscription("Raven/DocumentsByEntityName")
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				IndexChangeNotification indexChangeNotification;
				Assert.True(list.TryTake(out indexChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("Raven/DocumentsByEntityName", indexChangeNotification.Name);
				Assert.Equal(indexChangeNotification.Type, IndexChangeTypes.MapCompleted);
			}
		}
	}
}