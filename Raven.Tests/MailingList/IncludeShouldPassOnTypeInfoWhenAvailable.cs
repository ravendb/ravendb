using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class IncludeShouldPassOnTypeInfoWhenAvailable : RavenTest
	{
		[Fact]
		public void Test()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079/" }.Initialize())
			{
				var docId = default(string);
				var relatedDocId = default(string);

				using (var session = store.OpenSession())
				{
					var relatedDoc = new SomeRelatedDoc { Name = "Foo" };
					session.Store(relatedDoc);

					relatedDocId = relatedDoc.Id;

					var doc = new SomeDoc
					{
						Name = "Bar",
						RelatedDocId = relatedDocId
					};

					session.Store(doc);

					docId = doc.Id;

					session.SaveChanges();
				}

				// remove RavenClrType type from metadata
				var relatedDocJson = store.DatabaseCommands.Get(relatedDocId);
				relatedDocJson.Metadata.Remove(Constants.RavenClrType);
				store.DatabaseCommands.Put(relatedDocId, null, relatedDocJson.DataAsJson, relatedDocJson.Metadata);

				using (var session = store.OpenSession())
				{
					var doc = session.Include<SomeDoc, SomeRelatedDoc>(x => x.RelatedDocId).Load(docId);
					var relatedDoc = session.Load<SomeRelatedDoc>(relatedDocId);
					var relatedDocMetaData = session.Advanced.GetMetadataFor(relatedDoc);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotNull(doc);
					Assert.NotNull(relatedDoc);
					Assert.False(relatedDocMetaData.ContainsKey(Constants.RavenClrType));
				}
			}
		}
	}

	public class SomeDoc
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string RelatedDocId { get; set; }
	}

	public class SomeRelatedDoc
	{
		public string Id { get; set; }
		public string Name { get; set; }
	}
}
