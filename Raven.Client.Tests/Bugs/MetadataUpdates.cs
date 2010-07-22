using Xunit;
using Newtonsoft.Json.Linq;

namespace Raven.Client.Tests.Bugs
{
	public class MetadataUpdates : BaseClientTest
	{
		[Fact]
		public void WhenModifyingMetadata_ThenSavesChanges()
		{
			using(var store = NewDocumentStore())
			{
				string id = null;
				// Initial create
				using (var session = store.OpenSession())
				{
					var foo = new IndexWithTwoProperties.Foo { Value = "hello" };
					session.Store(foo);
					session.SaveChanges();
					id = foo.Id;
				}

				// Update metadata 
				using (var session = store.OpenSession())
				{
					var foo = session.Load<IndexWithTwoProperties.Foo>(id);
					var metadata = session.GetMetadataFor(foo);

					metadata["foo"] = "bar";
					session.SaveChanges();
				}

				// Entity should have the updated metadata now.
				using (var session = store.OpenSession())
				{
					var foo = session.Load<IndexWithTwoProperties.Foo>(id);
					var metadata = session.GetMetadataFor(foo);

					Assert.Equal("bar", metadata["foo"].Value<string>());
				}
			}
		}
	}
}