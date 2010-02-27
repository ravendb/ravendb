using System;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Extensions;
using Xunit;

namespace Rhino.DivanDB.Tests.Storage
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
            string documentId = db.Put(JObject.Parse("{_id: '1', first_name: 'ayende', last_name: 'rahien'}"));
            Assert.Equal("1", documentId);
        }

        [Fact]
        public void When_creating_document_with_no_id_specified_will_return_guid_as_id()
        {
            var documentId = db.Put(JObject.Parse("{first_name: 'ayende', last_name: 'rahien'}"));
            Assert.DoesNotThrow(() => new Guid(documentId));
        }

        [Fact]
        public void When_creating_documents_with_no_id_specified_will_return_guids_in_sequencal_order()
        {
            var documentId1 = db.Put(JObject.Parse("{first_name: 'ayende', last_name: 'rahien'}"));
            var documentId2 = db.Put(JObject.Parse("{first_name: 'ayende', last_name: 'rahien'}"));
            Assert.Equal(1, new Guid(documentId2).CompareTo(new Guid(documentId1)));
        }

        [Fact]
        public void Can_create_and_read_document()
        {
            db.Put(JObject.Parse("{_id: '1', first_name: 'ayende', last_name: 'rahien'}"));
            JObject document = db.Get("1").ToJson();

            Assert.Equal("1", document.Value<string>("_id"));
            Assert.Equal("ayende", document.Value<string>("first_name"));
            Assert.Equal("rahien", document.Value<string>("last_name"));
        }

        [Fact]
        public void Can_edit_document()
        {
            db.Put(JObject.Parse("{_id: '1', first_name: 'ayende', last_name: 'rahien'}"));
            db.Put(JObject.Parse("{_id: '1', first_name: 'ayende2', last_name: 'rahien2'}"));
            JObject document = db.Get("1").ToJson();

            Assert.Equal("1", document.Value<string>("_id"));
            Assert.Equal("ayende2", document.Value<string>("first_name"));
            Assert.Equal("rahien2", document.Value<string>("last_name"));
        }

        [Fact]
        public void Can_delete_document()
        {
            db.Put(JObject.Parse("{_id: '1', first_name: 'ayende', last_name: 'rahien'}"));
            db.Delete("1");

            Assert.Null(db.Get("1"));
        }

        [Fact]
        public void Can_query_document_by_id_when_having_multiple_documents()
        {
            db.Put(JObject.Parse("{_id: '1', first_name: 'ayende', last_name: 'rahien'}"));
            db.Put(JObject.Parse("{_id: '21', first_name: 'ayende2', last_name: 'rahien2'}"));
            JObject document = db.Get("21").ToJson();

            Assert.Equal("21", document.Value<string>("_id"));
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