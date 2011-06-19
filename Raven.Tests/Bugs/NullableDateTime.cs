using System;
using Newtonsoft.Json;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class NullableDateTime : LocalClientTest
	{
		[Fact]
		public void WillNotIncludeItemsWithNullDate()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new WithNullableDateTime());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var withNullableDateTimes = session.Query<WithNullableDateTime>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.CreatedAt > new DateTime(2000, 1, 1) && x.CreatedAt != null)
						.ToList();
					Assert.Empty(withNullableDateTimes);
				}
			}
		}

		[Fact]
		public void CanLoadFromIndex()
		{
			using(var documentStore = NewDocumentStore())
			{
				using (IDocumentSession session = documentStore.OpenSession())
				{
					IndexCreation.CreateIndexes(typeof (Doc).Assembly, documentStore);
					session.Store(new Doc {Id = "test/doc1", Date = DateTime.UtcNow});
					session.Store(new Doc {Id = "test/doc2", Date = null});
					session.SaveChanges();

				}
				

				using(var session = documentStore.OpenSession())
				{
					session
						.Query<Doc, UnsetDocs>()
						.Customize(x => x.WaitForNonStaleResults())
						.AsProjection<DocSummary>()
						.ToArray();
				}
			}

		}
		public class WithNullableDateTime
		{
			public string Id { get; set; }
			public DateTime? CreatedAt { get; set; }
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
	}
}