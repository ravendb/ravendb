using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexWithWhere : RavenTestBase
    {
        private class Document
        {
            public string Title { get; set; }

            public string Description { get; set; }

            public bool IsDeleted { get; set; }
        }

        private class Index_ByDescriptionAndTitle : AbstractIndexCreationTask<Document>
        {
            public Index_ByDescriptionAndTitle()
            {
                Map = docs => from doc in docs
                              where doc.Title == "dfsdfsfd"
                              select new { doc.Description, doc.Title };
            }
        }

        private class Index_ByDescriptionAndTitle2 : AbstractIndexCreationTask<Document>
        {
            public Index_ByDescriptionAndTitle2()
            {
                Map = docs => from doc in docs
                              where
                                doc.IsDeleted == false
                              select new { doc.Description, doc.Title };
            }
        }

        [Fact]
        public void CanCreateIndex()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.PrettifyGeneratedLinqExpressions = false;
                }
            }))
            {
                new Index_ByDescriptionAndTitle().Execute(store);

                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("Index/ByDescriptionAndTitle"));
                Assert.Equal(@"docs.Documents.Where(doc => doc.Title == ""dfsdfsfd"").Select(doc => new {
    Description = doc.Description,
    Title = doc.Title
})".Replace("\r\n", Environment.NewLine), indexDefinition.Maps.First());
            }
        }

        [Fact]
        public void CanCreateIndex2()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.PrettifyGeneratedLinqExpressions = false;
                }
            }))
            {
                new Index_ByDescriptionAndTitle2().Execute(store);

                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("Index/ByDescriptionAndTitle2"));
                Assert.Equal(@"docs.Documents.Where(doc => doc.IsDeleted == false).Select(doc => new {
    Description = doc.Description,
    Title = doc.Title
})".Replace("\r\n", Environment.NewLine), indexDefinition.Maps.First());
            }
        }
    }
}
