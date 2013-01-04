using System;
using Xunit;

namespace Raven.Tests.Json
{
	public class JsonUri : RavenTest
	{
		public class ObjectWithUri
		{
			public Uri Url { get; set; }
		}

		[Fact]
		public void can_serialize_uri_props_correctly()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ObjectWithUri {Url = new Uri("http://test.com/%22foo+bar%22")}, "test");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var uri = session.Load<ObjectWithUri>("test");
					Assert.NotNull(uri);
				}
			}
		}
	}
}
