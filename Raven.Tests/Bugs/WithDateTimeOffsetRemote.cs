using System;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WithDateTimeOffsetRemote : RemoteClientTest
	{
		[Fact]
		public void CanStoreAndGetValues()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				var expected = new DateTimeOffset(2010, 11, 10, 19, 13, 18, 26, TimeSpan.FromHours(2));
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
		}

		private class FooBar
		{
			public DateTimeOffset Foo { get; set; }
		}
	}
}