using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Tests.Common;

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
				IDisposable subscription1 = store.Changes()
				                                 .Task.Result
				                                 .ForDocument("TestDocuments/1").Task.Result
				                                 .Subscribe(d =>
				                                 {
					                                 notification1 = d;
					                                 resetEvent1.Set();
				                                 });
				using (IDocumentSession session = store.OpenSession())
				{
					var doc = session.Load<TestDocument>("TestDocuments/1");
					doc.Name = "NewName1";
					session.SaveChanges();
				}

				Assert.True(resetEvent1.WaitOne(5000));
				Assert.NotNull(notification1);

				subscription1.Dispose();

				DocumentChangeNotification notification2 = null;
				var resetEvent2 = new ManualResetEvent(false);
				IDisposable subscription2 = store.Changes().Task.Result
				                                 .ForDocument("TestDocuments/1").Task.Result
				                                 .Subscribe(d =>
				                                 {
					                                 notification2 = d;
					                                 resetEvent2.Set();
				                                 });

				using (IDocumentSession session = store.OpenSession())
				{
					var doc = session.Load<TestDocument>("TestDocuments/1");
					doc.Name = "NewName2";
					session.SaveChanges();
				}

				Assert.True(resetEvent2.WaitOne(500));
				Assert.NotNull(notification2);

				subscription2.Dispose();
			}
		}

		public class TestDocument
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}
	}
}