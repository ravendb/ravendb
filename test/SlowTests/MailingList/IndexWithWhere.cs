using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
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

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanCreateIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.PrettifyGeneratedLinqExpressions = false;
                new Index_ByDescriptionAndTitle().Execute(store);

                var indexDefinition = store.DatabaseCommands.GetIndex("Index/ByDescriptionAndTitle");
                Assert.Equal(@"docs.Documents.Where(doc => doc.Title == ""dfsdfsfd"").Select(doc => new {
    Description = doc.Description,
    Title = doc.Title
})", indexDefinition.Maps.First());
            }
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanCreateIndex2()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.PrettifyGeneratedLinqExpressions = false;
                new Index_ByDescriptionAndTitle2().Execute(store);

                var indexDefinition = store.DatabaseCommands.GetIndex("Index/ByDescriptionAndTitle2");
                Assert.Equal(@"docs.Documents.Where(doc => doc.IsDeleted == false).Select(doc => new {
    Description = doc.Description,
    Title = doc.Title
})", indexDefinition.Maps.First());
            }
        }
    }
}
