using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Xunit;

namespace Raven.Tests.Storage
{
	public class CreateUpdateDeleteDocuments : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public CreateUpdateDeleteDocuments()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

		[Fact]
		public void When_creating_document_with_id_specified_will_return_specified_id()
		{
			var documentId = db.Put("1", Guid.Empty, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"),
			                        new JObject(), null);
			Assert.Equal("1", documentId.Key);
		}

		[Fact]
		public void Can_get_id_from_document_metadata()
		{
			db.Put("1", Guid.Empty, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"),
			       new JObject(), null);
			Assert.Equal("1", db.Get("1", null).Metadata["@id"].Value<string>());
		}

		[Fact]
		public void When_creating_document_with_no_id_specified_will_return_guid_as_id()
		{
			var documentId = db.Put(null, Guid.Empty, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"),
                                    new JObject(), null);
			Assert.DoesNotThrow(() => new Guid(documentId.Key));
		}

		[Fact]
		public void Can_create_and_read_document()
		{
            db.Put("1", Guid.Empty, JObject.Parse("{  first_name: 'ayende', last_name: 'rahien'}"), new JObject(), null);
            var document = db.Get("1", null).ToJson();

			Assert.Equal("ayende", document.Value<string>("first_name"));
			Assert.Equal("rahien", document.Value<string>("last_name"));
		}

		[Fact]
		public void Can_edit_document()
		{
            db.Put("1", Guid.Empty, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject(), null);

            db.Put("1", db.Get("1", null).Etag, JObject.Parse("{ first_name: 'ayende2', last_name: 'rahien2'}"), new JObject(), null);
            var document = db.Get("1", null).ToJson();

			Assert.Equal("ayende2", document.Value<string>("first_name"));
			Assert.Equal("rahien2", document.Value<string>("last_name"));
		}

		[Fact]
		public void Can_delete_document()
		{
            db.Put("1", Guid.Empty, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject(), null);
            var document = db.Get("1", null);
            db.Delete("1", document.Etag, null);

            Assert.Null(db.Get("1", null));
		}

		[Fact]
		public void Can_query_document_by_id_when_having_multiple_documents()
		{
            db.Put("1", Guid.Empty, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject(), null);
            db.Put("21", Guid.Empty, JObject.Parse("{ first_name: 'ayende2', last_name: 'rahien2'}"), new JObject(), null);
            var document = db.Get("21", null).ToJson();

			Assert.Equal("ayende2", document.Value<string>("first_name"));
			Assert.Equal("rahien2", document.Value<string>("last_name"));
		}

		[Fact]
		public void Querying_by_non_existant_document_returns_null()
		{
            Assert.Null(db.Get("1", null));
		}
	}
}
