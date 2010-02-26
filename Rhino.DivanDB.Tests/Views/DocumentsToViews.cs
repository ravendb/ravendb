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
        }

        [Fact]
        public void Can_Read_values_when_two_views_exist()
        {
            db.AddView(
               @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.other};
");  
            db.AddView(
               @"var pagesByTitle2 = 
    from doc in docs
    where doc.type == ""page""
    select new { doc.some };
");
            db.Put(JObject.Parse("{_id: '1', type: 'page', some: 'val', other: 'var', content: 'this is the content', title: 'hello world', size: 5}"));

            var docs = db.Query("pagesByTitle2", "some:val");
            Assert.Equal(1, docs.Length);
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}