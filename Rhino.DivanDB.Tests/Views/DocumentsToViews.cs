using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Tests.Storage;
using Xunit;
using System.Linq;

namespace Rhino.DivanDB.Tests.Views
{
    public class DocumentsToViews : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public DocumentsToViews()
        {
            db = new DocumentDatabase("divan.db.test.esent");
        }

        [Fact]
        public void Adding_a_document_will_add_it_to_the_queue_of_existing_views()
        {
            db.AddView(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            db.AddDocument(JObject.Parse("{_id: '1', type: 'page', content: 'this is the content', title: 'hello world', size: 5}"));

            db.Storage.Batch(actions =>
            {
                var id = actions.QueuedDocumentsFor("pagesByTitle").ToArray()[0];
                Assert.Equal("1", id);
            });
        }

        [Fact]
        public void Adding_a_view_will_queue_all_documents_for_this_view()
        {
            db.AddDocument(JObject.Parse("{_id: '1', type: 'page', content: 'this is the content', title: 'hello world', size: 5}"));

            db.AddView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            db.Storage.Batch(actions =>
            {
                var id = actions.QueuedDocumentsFor("pagesByTitle").ToArray()[0];
                Assert.Equal("1", id);
            });
        }

        [Fact]
        public void Can_Read_values_when_two_views_exist()
        {
            db.AddView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");  
            db.AddView(
               @"var pagesByTitle2 = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.other, Value = doc.content, Size = (int)doc.size };
");
            db.AddDocument(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));

            db.ProcessQueuedDocuments();

            var docs = db.ViewRecordsByNameAndKey("pagesByTitle2", "var");
            Assert.Equal(1, docs.Length);

        }

        [Fact]
        public void Can_read_values_from_view()
        {
          

            db.AddView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            db.AddDocument(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));

            db.ProcessQueuedDocuments();

            var docs = db.ViewRecordsByNameAndKey("pagesByTitle", "hello world");
            Assert.Equal(1, docs.Length);
            Assert.Equal(@"{
  ""Key"": ""hello world"",
  ""Value"": ""this is the content"",
  ""Size"": 5
}", docs[0].ToString());
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}