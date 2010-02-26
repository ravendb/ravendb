using System;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Tests.Storage;
using Xunit;

namespace Rhino.DivanDB.Tests.Views
{
    public class DocumentsToViews : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public DocumentsToViews()
        {
            db = new DocumentDatabase("divan.db.test.esent");
            db.SpinBackgroundWorkers();
        }

        [Fact]
        public void Can_Read_values_from_view()
        {
            db.PutView(
               @"var pagesByTitle2 = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.some };
");
            db.Put(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));


            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle2", "some:val");
            } while (docs.IsStale);
            Assert.Equal(1, docs.Results.Length);
        }

        [Fact]
        public void Can_Read_values_when_two_views_exist()
        {
            db.PutView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.other};
");
            db.PutView(
               @"var pagesByTitle2 = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.some };
");
            db.Put(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));


            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle2", "some:val");
            } while (docs.IsStale);
            Assert.Equal(1, docs.Results.Length);
        }

        [Fact]
        public void Updating_a_view_will_result_in_new_values()
        {
            db.PutView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.other};
");
            db.PutView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.other };
");
            db.Put(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));


            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle", "other:var");
            } while (docs.IsStale);
            Assert.Equal(1, docs.Results.Length);
        }

        [Fact]
        public void Can_read_values_from_views_of_documents_already_in_db()
        {
            db.Put(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));

            db.PutView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.other };
");
            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle", "other:var");
            } while (docs.IsStale);
            Assert.Equal(1, docs.Results.Length);
        }

        [Fact]
        public void Can_read_values_from_views_of_documents_already_in_db_when_multiple_docs_exists()
        {
            db.Put(JObject.Parse("{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));
            db.Put(JObject.Parse("{type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));

            db.PutView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.other };
");
            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle", "other:var");
            } while (docs.IsStale);
            Assert.Equal(2, docs.Results.Length);
        }


        public void Dispose()
        {
            db.Dispose();
        }
    }
}