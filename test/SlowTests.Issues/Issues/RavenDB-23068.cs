using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.IndexMerging;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23068(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task JavaScriptIndexesShouldBeIncludedInUnmergables()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new TestDoc { Name = "John", SecondName = "Doe" });
        await session.SaveChangesAsync();
        var csharpIndex = new CsharpIndex();
        var csharpIndex2 = new CSharpIndex2();
        var jsIndex = new JsIndex();
        await csharpIndex.ExecuteAsync(store);
        await csharpIndex2.ExecuteAsync(store);
        await jsIndex.ExecuteAsync(store);
        
        var definitionByIndexName = new Dictionary<string, IndexDefinition>();
        var database = await GetDatabase(store.Database);

        foreach (var index in database.IndexStore.GetIndexes())
        {
            definitionByIndexName[index.Name] = index.GetIndexDefinition();
        }
        
        var indexMerger = new IndexMerger(definitionByIndexName);
        var mergeSuggestions = indexMerger.ProposeIndexMergeSuggestions();
        Assert.NotEmpty(mergeSuggestions.Unmergables);
        Assert.Contains(jsIndex.IndexName, mergeSuggestions.Unmergables);
        var jsIndexReason = mergeSuggestions.Unmergables[jsIndex.IndexName];
        Assert.Equal("Cannot merge JavaScript indexes.", jsIndexReason);
        
        Assert.Empty(mergeSuggestions.Errors);
        Assert.NotEmpty(mergeSuggestions.Suggestions);
        var suggestion = mergeSuggestions.Suggestions.First();
        Assert.Equal(2, suggestion.CanMerge.Count);
        Assert.Contains(csharpIndex.IndexName, suggestion.CanMerge);
        Assert.Contains(csharpIndex2.IndexName, suggestion.CanMerge);
    }
    
    private class TestDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string SecondName { get; set; }
    }

    private class CsharpIndex : AbstractIndexCreationTask<TestDoc>
    {
        public CsharpIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Name };
        }
    }
    
    private class CSharpIndex2 : AbstractIndexCreationTask<TestDoc>
    {
        public CSharpIndex2()
        {
            Map = docs => from doc in docs
                select new { doc.SecondName };
        }
    }

    private class JsIndex : AbstractJavaScriptIndexCreationTask
    {
        public JsIndex()
        {
            Maps =
            [
                @"map(""TestDocs"", (doc) => {
    
        return {
            Name: doc.Name,
            SecondName: doc.SecondName
        };
})"
            ];
        }
    }
}
