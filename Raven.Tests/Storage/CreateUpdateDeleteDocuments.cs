using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Xunit;

namespace Raven.Tests.Storage
{
    public class CreateUpdateDeleteDocuments : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public CreateUpdateDeleteDocuments()
        {
            db = new DocumentDatabase("divan.db.test.esent");
        }

        [Fact]
        public void When_creating_document_with_id_specified_will_return_specified_id()
        {
            string documentId = db.Put("1",JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            Assert.Equal("1", documentId);
        }

        [Fact]
        public void When_creating_document_with_no_id_specified_will_return_guid_as_id()
        {
            string documentId = db.Put(null, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            Assert.DoesNotThrow(() => new Guid(documentId));
        }

        [Fact]
        public void When_creating_documents_with_no_id_specified_will_return_guids_in_sequencal_order()
        {
            string documentId1 = db.Put(null, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            string documentId2 = db.Put(null, JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            Assert.Equal(1, new Guid(documentId2).CompareTo(new Guid(documentId1)));
        }

        [Fact]
        public void Can_create_and_read_document()
        {
            db.Put("1", JObject.Parse("{  first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            JObject document = db.Get("1").ToJson();

            Assert.Equal("ayende", document.Value<string>("first_name"));
            Assert.Equal("rahien", document.Value<string>("last_name"));
        }

        [Fact]
        public void Can_edit_document()
        {
            db.Put("1", JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            db.Put("1", JObject.Parse("{ first_name: 'ayende2', last_name: 'rahien2'}"), new JObject());
            JObject document = db.Get("1").ToJson();

            Assert.Equal("ayende2", document.Value<string>("first_name"));
            Assert.Equal("rahien2", document.Value<string>("last_name"));
        }

        [Fact]
        public void Can_delete_document()
        {
            db.Put("1", JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            db.Delete("1");

            Assert.Null(db.Get("1"));
        }

        [Fact]
        public void Can_query_document_by_id_when_having_multiple_documents()
        {
            db.Put("1",JObject.Parse("{ first_name: 'ayende', last_name: 'rahien'}"), new JObject());
            db.Put("21",JObject.Parse("{ first_name: 'ayende2', last_name: 'rahien2'}"), new JObject());
            JObject document = db.Get("21").ToJson();

            Assert.Equal("ayende2", document.Value<string>("first_name"));
            Assert.Equal("rahien2", document.Value<string>("last_name"));
        }

        [Fact]
        public void Querying_by_non_existant_document_returns_null()
        {
            Assert.Null(db.Get("1"));
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}