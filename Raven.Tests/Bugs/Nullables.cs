using Raven.Client.Connection;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Nullables : LocalClientTest
	{
		[Fact]
		public void CanWriteNullablesProperly()
		{
			using (var store = NewDocumentStore())
			{
				//_db is document store
				var fooJson = RavenJObject.FromObject(new Foo
				{
					Id = "foo/10",
					BarItem = new Bar {Name = "My Bar Item X"}

				}, store.Conventions.CreateSerializer());

				var fooObject = fooJson.Deserialize(typeof (Foo),
				                                    store.Conventions);
				decimal? size = ((Foo) fooObject).BarItem.Size;

				Assert.Null(size);
			}
		}


		public class Foo
		{
			public string Id { get; set; }
			public Bar BarItem { get; set; }
		}

		public class Bar
		{
			public string Name { get; set; }
			public decimal? Size { get; set; }
		}

	}
}