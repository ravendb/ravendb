using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.MailingList
{

	public class FailingChangesApiTests : RavenTest
	{
		[Fact]
		public void Should_get_independent_notification_subscriptions()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new TestDocument { Name = "Name" });
					session.SaveChanges();
				}

				DocumentChangeNotification notification1 = null;
				var resetEvent1 = new ManualResetEvent(false);
				//Logger.Info("Creating first subscription");
				IDisposable subscription1 = store.Changes()
																	.ForDocument("TestDocuments/1")
																	.Subscribe(d =>
																	{
																		//Logger.Info("Received change notification 1: DocumentId={0}", d.Id);
																		notification1 = d;
																		resetEvent1.Set();
																	});
				//Logger.Info("Created first subscription");

				using (IDocumentSession session = store.OpenSession())
				{
					//Logger.Info("Updating document first time");
					var doc = session.Load<TestDocument>("TestDocuments/1");
					doc.Name = "NewName1";
					session.SaveChanges();
					//Logger.Info("Updated document first time");
				}

				Assert.True(resetEvent1.WaitOne(5000));
				Assert.NotNull(notification1);

				//Logger.Info("Disposing first subscription");
				subscription1.Dispose();
				//Logger.Info("Disposed first subscription");

				Thread.Sleep(5000);

				DocumentChangeNotification notification2 = null;
				var resetEvent2 = new ManualResetEvent(false);
				//Logger.Info("Creating second subscription");
				IDisposable subscription2 = store.Changes()
																	.ForDocument("TestDocuments/1")
																	.Subscribe(d =>
																	{
																		//Logger.Info("Received change notification 2: DocumentId={0}", d.Id);
																		notification2 = d;
																		resetEvent2.Set();
																	});
				//Logger.Info("Created second subscription");

				using (IDocumentSession session = store.OpenSession())
				{
					//Logger.Info("Updating document second time");
					var doc = session.Load<TestDocument>("TestDocuments/1");
					doc.Name = "NewName2";
					session.SaveChanges();
					//Logger.Info("Updated document second time");
				}

				Assert.True(resetEvent2.WaitOne(5000));
				Assert.NotNull(notification2);

				//Logger.Info("Disposing second subscription");
				subscription2.Dispose();
				//Logger.Info("Disposed second subscription");
			}
		}

		public class TestDocument
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}
	}
}