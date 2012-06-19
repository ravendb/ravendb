using System;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanStoreAndGetDateTimeOffset : RemoteClientTest
	{
		[Fact]
		public void WithEmbedded()
		{
			using (var store = NewDocumentStore())
				ExecuteTest(store);
		}

		[Fact]
		public void WithRemote()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
				ExecuteTest(store);
		}

		private void ExecuteTest(IDocumentStore store)
		{
			var expected = new DateTimeOffset(2010, 11, 10, 19, 13, 26, 509, TimeSpan.FromHours(2));
			using (var session = store.OpenSession())
			{
				session.Store(new FooBar {Foo = expected});
				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				var fooBar = session.Load<FooBar>("foobars/1");
				Assert.Equal(expected, fooBar.Foo);
			}
		}

		private class FooBar
		{
			public DateTimeOffset Foo { get; set; }
		}
	}
}