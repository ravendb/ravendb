//-----------------------------------------------------------------------
// <copyright file="MetadataUpdates.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Xunit;
using Raven.Json.Linq;

namespace Raven.Tests.Bugs
{
	public class MetadataUpdates : LocalClientTest
	{

		[Fact]
		public void WhenUpdating_ThenPreservesMetadata()
		{
			using (var store = NewDocumentStore())
			{


				var foo = new IndexWithTwoProperties.Foo {Value = "hello"};



				using (var session = store.OpenSession())
				{
					session.Store(foo);
                    session.Advanced.GetMetadataFor(foo)["bar"] = "baz";
					session.SaveChanges();
				}



				// When we load, the metadata is there.
				using (var session = store.OpenSession())
				{
					var saved = session.Load<IndexWithTwoProperties.Foo>(foo.Id);
                    var metadata = session.Advanced.GetMetadataFor(saved);
					Assert.Equal("baz", metadata["bar"].Value<string>());


				}

				// When we update, we kill the existing metadata.
				using (var session = store.OpenSession())
				{
					var saved = session.Load<IndexWithTwoProperties.Foo>(foo.Id);
					saved.Value = "bye";
					session.Store(saved);
					session.SaveChanges();
				}

				// When we load, the metadata is gone.


				using (var session = store.OpenSession())
				{
					var saved = session.Load<IndexWithTwoProperties.Foo>(foo.Id);


                    var metadata = session.Advanced.GetMetadataFor(saved);
					// FAILS HERE
					var jToken = metadata["bar"];
					Assert.Equal("baz", jToken.Value<string>());
				}
			}
		}


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
                    var metadata = session.Advanced.GetMetadataFor(foo);

					metadata["foo"] = "bar";
					session.SaveChanges();
				}

				// Entity should have the updated metadata now.
				using (var session = store.OpenSession())
				{
					var foo = session.Load<IndexWithTwoProperties.Foo>(id);
                    var metadata = session.Advanced.GetMetadataFor(foo);

					Assert.Equal("bar", metadata["foo"].Value<string>());
				}
			}
		}
	}
}
