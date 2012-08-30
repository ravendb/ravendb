using System;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Nullables : RavenTest
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
					BarItem = new Bar { Name = "My Bar Item X" }

				}, store.Conventions.CreateSerializer());

				var fooObject = fooJson.Deserialize(typeof(Foo),
													store.Conventions);
				decimal? size = ((Foo)fooObject).BarItem.Size;

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


		public class Doc
		{
			public string Id { get; set; }
			public DateTime? Date { get; set; }
		}

		public class DocSummary
		{
			public string Id { get; set; }
			public DateTime? MaxDate { get; set; }
		}

		public class UnsetDocs : AbstractIndexCreationTask<Doc, DocSummary>
		{
			public UnsetDocs()
			{
				Map = docs =>
						from doc in docs
						select new
						{
							doc.Id,
							MaxDate = doc.Date
						};
				Store(x => x.MaxDate, FieldStorage.Yes);
			}
		}

		[Fact]
		public void CanLoadFromIndex()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					new UnsetDocs().Execute(documentStore);
					session.Store(new Doc { Id = "test/doc1", Date = SystemTime.UtcNow });
					session.Store(new Doc { Id = "test/doc2", Date = null });
					session.SaveChanges();

				}

				using (var session = documentStore.OpenSession())
				{
					session
						.Query<Doc, UnsetDocs>()
						.Customize(x => x.WaitForNonStaleResults())
						.AsProjection<DocSummary>()
						.ToArray();
				}
			}


		}
	}
}
