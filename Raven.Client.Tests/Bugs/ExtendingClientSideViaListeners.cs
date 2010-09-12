using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class ExtendingClientSideViaListeners : LocalClientTest
	{
		[Fact]
		public void CanFailSave()
		{
			using (var store = NewDocumentStore()
				.RegisterListener(new FailStore()))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Email());
					Assert.Throws<NotImplementedException>(() => session.SaveChanges());
				}
			}
		}

		[Fact]
		public void CanFailDelete()
		{
			using (var store = NewDocumentStore()
				.RegisterListener(new FailDelete()))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Email());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Delete(session.Load<Email>("emails/1"));
					Assert.Throws<NotImplementedException>(() => session.SaveChanges());
				}
			}
		}

		public class Email
		{
			public string Id { get; set; }
		}
	}

	public class FailDelete : IDocumentDeleteListener
	{
		public void BeforeDelete(string key, object entityInstance, JObject metadata)
		{
			throw new NotImplementedException();
		}
	}

	public class FailStore : IDocumentStoreListener
	{
		public void BeforeStore(string key, object entityInstance, JObject metadata)
		{
			throw new NotImplementedException();
		}

		public void AfterStore(string key, object entityInstance, JObject metadata)
		{
			throw new NotImplementedException();
		}
	}
}