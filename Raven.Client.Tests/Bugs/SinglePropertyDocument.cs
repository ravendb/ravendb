using System;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class SinglePropertyDocument : LocalClientTest
	{
		[Fact]
		public void CanSaveDocumentWithJustId()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Email());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotNull(session.Load<Email>("emails/1"));
				}
			}
		}

		public class Email
		{
			public string Id { get; set; }
		}
	}
}
