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

            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors.First(x => x.IndexName == secondIndex.IndexName).Message);
            Assert.NotNull(indexMergeSuggestions.Errors.First(x => x.IndexName == secondIndex.IndexName).StackTrace);
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

            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors.First(x => x.IndexName == firstIndex.IndexName).Message);
            Assert.NotNull(indexMergeSuggestions.Errors.First(x => x.IndexName == firstIndex.IndexName).StackTrace);
            
            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors.First(x => x.IndexName == thirdIndex.IndexName).Message);
            Assert.NotNull(indexMergeSuggestions.Errors.First(x => x.IndexName == thirdIndex.IndexName).StackTrace);
            
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

            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors.First(x => x.IndexName == firstIndex.IndexName).Message);
            Assert.NotNull(indexMergeSuggestions.Errors.First(x => x.IndexName == firstIndex.IndexName).StackTrace);
            
            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors.First(x => x.IndexName == secondIndex.IndexName).Message);
            Assert.NotNull(indexMergeSuggestions.Errors.First(x => x.IndexName == secondIndex.IndexName).StackTrace);
            
            Assert.Equal(ExceptionMessage, indexMergeSuggestions.Errors.First(x => x.IndexName == thirdIndex.IndexName).Message);
            Assert.NotNull(indexMergeSuggestions.Errors.First(x => x.IndexName == thirdIndex.IndexName).StackTrace);
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task TestIndexesThatFailToBeMerged()
    {
        using (var store = GetDocumentStore())
        {
            var firstFailingIndex = new FirstFailingIndex();
            var secondFailingIndex = new SecondFailingIndex();
            var workingIndex = new ThirdIndex();
            
            await firstFailingIndex.ExecuteAsync(store);
            await secondFailingIndex.ExecuteAsync(store);
            await workingIndex.ExecuteAsync(store);
            
            Indexes.WaitForIndexing(store);
            
            var database = await GetDatabase(store.Database);

            var indexes = new Dictionary<string, IndexDefinition>();
            foreach (var index in database.IndexStore.GetIndexes())
                indexes[index.Name] = index.GetIndexDefinition();

            var indexMergeResults = new IndexMerger(indexes).ProposeIndexMergeSuggestions();
            
            Assert.Equal(1, indexMergeResults.Suggestions.Count);
            Assert.Equal(0, indexMergeResults.Unmergables.Count);
            Assert.Equal(0, indexMergeResults.Errors.Count);
            
            Assert.True(indexMergeResults.Suggestions.First().CanMerge.Contains(firstFailingIndex.IndexName));
            Assert.True(indexMergeResults.Suggestions.First().CanMerge.Contains(secondFailingIndex.IndexName));
            Assert.True(indexMergeResults.Suggestions.First().CanMerge.Contains(workingIndex.IndexName));
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

    private class FirstFailingIndex : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition() { Maps = new HashSet<string>() { "from u in docs.Users\nselect new\n{\n    FirstName = u.Details?.FirstName ?? \"\"\n}" } };
        }
    }

    private class SecondFailingIndex : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition() { Maps = new HashSet<string>() { "from u in docs.Users\nselect new\n{\n    LastName = u.Details != null ? u.Details.LastName : \"\"\n}" } };
        }
    }
}
