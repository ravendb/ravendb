using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18588 : RavenTestBase
{
    public RavenDB_18588(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void EnumListIsProperlyCreatedInIndex()
    {
        using var store = GetDocumentStore();
        var id = "secret_id";
        {
            using var session = store.OpenSession();
            session.Store(new DocPage()
            {
                Language = Language.Csharp
            }, id);    
            session.SaveChanges();
        }
        
        new IndexWithList().Execute(store);
        Indexes.WaitForIndexing(store);

        {
            using var session = store.OpenSession();
            var docPage = session.Query<DocPage, IndexWithList>().Single(i => i.Language == Language.Csharp);
            Assert.Equal(docPage.Id, id);
        }
        
    }

    private class IndexWithList : AbstractIndexCreationTask<DocPage>
    {
        public IndexWithList()
        {
            Map = pages => from page in pages
                select new
                {
                    Language = new List<Language> {page.Language}
                };
        }
    }
    
    private enum Language
    {
        [Description("C#")]
        Csharp,
        [Description("Java")]
        Java,
    }
    
    private class DocPage
    {
        public string Id { get; set; }
        public Language Language { get; set; }
    }
}
