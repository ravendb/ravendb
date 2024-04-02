using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.IndexMerging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22178 : RavenTestBase
{
    private const string ExceptionMessage = "Intentionally caused for testing.";
    
    public RavenDB_22178(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task TestThreeIndexesNoMergingErrors()
    {
        using (var store = GetDocumentStore())
        {
            var firstIndex = new FirstIndex();
            var secondIndex = new SecondIndex();
            var thirdIndex = new ThirdIndex();
            
            await firstIndex.ExecuteAsync(store);
            await secondIndex.ExecuteAsync(store);
            await thirdIndex.ExecuteAsync(store);
            
            Indexes.WaitForIndexing(store);
            
            var database = await GetDatabase(store.Database);

            var indexMerger = PrepareIndexMerger(database, new List<string>(), ExceptionMessage);

            var indexMergeSuggestions = indexMerger.ProposeIndexMergeSuggestions();
            
            Assert.Equal(0, indexMergeSuggestions.Errors.Count);
            Assert.Equal(1, indexMergeSuggestions.Suggestions.Count);
            Assert.Equal(0, indexMergeSuggestions.Unmergables.Count);
            
            Assert.Equal(3, indexMergeSuggestions.Suggestions.First().CanMerge.Count);
            
            Assert.True(indexMergeSuggestions.Suggestions.First().CanMerge.Contains(firstIndex.IndexName));
            Assert.True(indexMergeSuggestions.Suggestions.First().CanMerge.Contains(secondIndex.IndexName));
            Assert.True(indexMergeSuggestions.Suggestions.First().CanMerge.Contains(thirdIndex.IndexName));
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task TestThreeIndexesWithMergingError()
    {
        using (var store = GetDocumentStore())
        {
            var firstIndex = new FirstIndex();
            var secondIndex = new SecondIndex();
            var thirdIndex = new ThirdIndex();
            
            await firstIndex.ExecuteAsync(store);
            await secondIndex.ExecuteAsync(store);
            await thirdIndex.ExecuteAsync(store);
            
            Indexes.WaitForIndexing(store);
            
            var database = await GetDatabase(store.Database);

            var indexMerger = PrepareIndexMerger(database, new List<string>(){ secondIndex.IndexName }, ExceptionMessage);

            var indexMergeSuggestions = indexMerger.ProposeIndexMergeSuggestions();
            
            Assert.Equal(1, indexMergeSuggestions.Errors.Count);
            Assert.Equal(1, indexMergeSuggestions.Suggestions.Count);
            Assert.Equal(0, indexMergeSuggestions.Unmergables.Count);

            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors[secondIndex.IndexName]);
            Assert.Equal(2, indexMergeSuggestions.Suggestions.First().CanMerge.Count);
            
            Assert.True(indexMergeSuggestions.Suggestions.First().CanMerge.Contains(firstIndex.IndexName));
            Assert.True(indexMergeSuggestions.Suggestions.First().CanMerge.Contains(thirdIndex.IndexName));
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task TestThreeIndexesWithTwoMergingErrors()
    {
        using (var store = GetDocumentStore())
        {
            var firstIndex = new FirstIndex();
            var secondIndex = new SecondIndex();
            var thirdIndex = new ThirdIndex();
            
            await firstIndex.ExecuteAsync(store);
            await secondIndex.ExecuteAsync(store);
            await thirdIndex.ExecuteAsync(store);
            
            Indexes.WaitForIndexing(store);
            
            var database = await GetDatabase(store.Database);

            var indexMerger = PrepareIndexMerger(database, new List<string>(){ firstIndex.IndexName, thirdIndex.IndexName }, ExceptionMessage);

            var indexMergeSuggestions = indexMerger.ProposeIndexMergeSuggestions();
            
            Assert.Equal(2, indexMergeSuggestions.Errors.Count);
            Assert.Equal(0, indexMergeSuggestions.Suggestions.Count);
            Assert.Equal(1, indexMergeSuggestions.Unmergables.Count);

            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors[firstIndex.IndexName]);
            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors[thirdIndex.IndexName]);
            
            Assert.True(indexMergeSuggestions.Unmergables.ContainsKey(secondIndex.IndexName));
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task TestThreeIndexesWithThreeMergingErrors()
    {
        using (var store = GetDocumentStore())
        {
            var firstIndex = new FirstIndex();
            var secondIndex = new SecondIndex();
            var thirdIndex = new ThirdIndex();
            
            await firstIndex.ExecuteAsync(store);
            await secondIndex.ExecuteAsync(store);
            await thirdIndex.ExecuteAsync(store);
            
            Indexes.WaitForIndexing(store);
            
            var database = await GetDatabase(store.Database);

            var indexMerger = PrepareIndexMerger(database, new List<string>(){ firstIndex.IndexName, secondIndex.IndexName, thirdIndex.IndexName }, ExceptionMessage);

            var indexMergeSuggestions = indexMerger.ProposeIndexMergeSuggestions();
            
            Assert.Equal(3, indexMergeSuggestions.Errors.Count);
            Assert.Equal(0, indexMergeSuggestions.Suggestions.Count);
            Assert.Equal(0, indexMergeSuggestions.Unmergables.Count);

            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors[firstIndex.IndexName]);
            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors[secondIndex.IndexName]);
            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors[thirdIndex.IndexName]);
        }
    }

    private static IndexMerger PrepareIndexMerger(DocumentDatabase database, List<string> indexNamesToThrowOn, string exceptionMessage)
    {
        var dic = new Dictionary<string, IndexDefinition>();

        foreach (var index in database.IndexStore.GetIndexes())
        {
            dic[index.Name] = index.GetIndexDefinition();
        }
        
        var indexMerger = new IndexMerger(dic);
        
        indexMerger.ForTestingPurposesOnly().IndexNamesToThrowOn = indexNamesToThrowOn;
        indexMerger.ForTestingPurposesOnly().OnTryMergeSelectExpressionsAndFields = (indexNames, currentIndexName) =>
        {
            if (indexNames.Contains(currentIndexName))
                throw new Exception(exceptionMessage);
        };

        return indexMerger;
    }
    
    private class FirstIndex : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition() { Maps = new HashSet<string>() { "from u in docs.Users\nselect new\n{\n    FirstName = u.Details.FirstName\n}" } };
        }
    }
    
    private class SecondIndex : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition() { Maps = new HashSet<string>() { "from u in docs.Users\nselect new\n{\n    LastName = u.Details.LastName\n}" } };
        }
    }
    
    private class ThirdIndex : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition() { Maps = new HashSet<string>() { "from u in docs.Users\nselect new\n{\n    Age = u.Details.Age\n}" } };
        }
    }
}
