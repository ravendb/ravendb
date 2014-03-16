// -----------------------------------------------------------------------
//  <copyright file="SimpleIndexMerging.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using System.Text.RegularExpressions;
using Raven.Database.Linq.PrivateExtensions;
using Xunit;

namespace Raven.Tests.IndexMerging
{
    public class SimpleIndexMerging : RavenTest
    {
        [Fact]
        public void WillSuggestMergeTwoSimpleIndexesForSameCollection()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test1", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Customer }"
                });
                store.DatabaseCommands.PutIndex("test2", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email }"
                });

                var suggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
                Assert.Equal(1, suggestions.Suggestions.Count);

                var mergeSuggestion = suggestions.Suggestions[0];
                Assert.Equal(new[] {"test1", "test2"}, mergeSuggestion.CanMerge);

                var suggestedIndexMap = mergeSuggestion.MergedIndex.Map.Replace("\r\n\t", " ");
                suggestedIndexMap = suggestedIndexMap.Replace("\t", " ").Trim();
                var expectedIndexMap = "from doc in docs.Orders select new { Customer = doc.Customer, Email = doc.Email }".Replace("\r\n\t", " ");
                expectedIndexMap = expectedIndexMap.Replace("\t", " ").Trim();

                suggestedIndexMap = Regex.Replace(suggestedIndexMap, @"\s+", " ");
                expectedIndexMap = Regex.Replace(expectedIndexMap, @"\s+", " ");
                Assert.Equal(expectedIndexMap,
                    suggestedIndexMap);
            }
        }

        [Fact]
        public void WillSuggestMergeTwoIndexesWithPropertiesForSameCollection()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test1", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Customer }",
                    SortOptions = new Dictionary<string, SortOptions> {{"Customer", SortOptions.String}}

                });
                store.DatabaseCommands.PutIndex("test2", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email }",
                    Stores = new Dictionary<string, FieldStorage> {{"Email", FieldStorage.Yes}},
                    SortOptions = new Dictionary<string, SortOptions> {{"Email", SortOptions.String}}

                });

                var suggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
                Assert.Equal(1, suggestions.Suggestions.Count);

                var mergeSuggestion = suggestions.Suggestions[0];
                Assert.Equal(new[] {"test1", "test2"}, mergeSuggestion.CanMerge);

                var suggestedIndexMap = mergeSuggestion.MergedIndex.Map.Replace("\r\n\t", " ");
                suggestedIndexMap = suggestedIndexMap.Replace("\t", " ").Trim();


                var expectedIndexMap = "from doc in docs.Orders select new { Customer = doc.Customer, Email = doc.Email }".Replace("\r\n\t", " ");
                expectedIndexMap = expectedIndexMap.Replace("\t", " ").Trim();

                suggestedIndexMap = System.Text.RegularExpressions.Regex.Replace(suggestedIndexMap, @"\s+", " ");
                expectedIndexMap = System.Text.RegularExpressions.Regex.Replace(expectedIndexMap, @"\s+", " ");
                Assert.Equal(expectedIndexMap,
                    suggestedIndexMap);

                var suggestedStoresDict = mergeSuggestion.MergedIndex.Stores.Keys.ToDictionary(key => key, key => mergeSuggestion.MergedIndex.Stores[key]);

                var suggestedSortDict = mergeSuggestion.MergedIndex.SortOptions.Keys.ToDictionary(key => key, key => mergeSuggestion.MergedIndex.SortOptions[key]);

                var expectedStoresDict = new Dictionary<string, FieldStorage> {{"Email", FieldStorage.Yes}};
                var expectedSortDict = new Dictionary<string, SortOptions> {{"Customer", SortOptions.String}, {"Email", SortOptions.String}};

                var result = DataDictionaryCompare(suggestedStoresDict, expectedStoresDict);
                Assert.Equal(true, result);

                result = DataDictionaryCompare(suggestedSortDict, expectedSortDict);
                Assert.Equal(true, result);

            }
        }

         [Fact]
        public void WillSuggestNoMergeTwoIndexesWithPropertiesForSameCollection()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test1", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Customer,o.Email }",
                    SortOptions = new Dictionary<string, SortOptions> { { "Customer", SortOptions.String }, { "Email", SortOptions.String } },


                });
                store.DatabaseCommands.PutIndex("test2", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email,o.Address }",
                    SortOptions = new Dictionary<string, SortOptions> { { "Email", SortOptions.String } }

                });
                store.DatabaseCommands.PutIndex("test3", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email,o.Tel }",
 
                });

                var suggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
                Assert.Equal(1, suggestions.Suggestions.Count);

                var mergeSuggestion = suggestions.Suggestions[0];
                Assert.Equal(new[] { "test1", "test2" }, mergeSuggestion.CanMerge);

                var suggestedIndexMap = mergeSuggestion.MergedIndex.Map.Replace("\r\n\t", " ");
                suggestedIndexMap = suggestedIndexMap.Replace("\t", " ").Trim();


                var expectedIndexMap = "from doc in docs.Orders select new {  Address = doc.Address, Customer = doc.Customer, Email = doc.Email }".Replace("\r\n\t", " ");
                expectedIndexMap = expectedIndexMap.Replace("\t", " ").Trim();

                suggestedIndexMap = Regex.Replace(suggestedIndexMap, @"\s+", " ");
                expectedIndexMap = Regex.Replace(expectedIndexMap, @"\s+", " ");
                Assert.Equal(expectedIndexMap,
                    suggestedIndexMap);

                var suggestedStoresDict = mergeSuggestion.MergedIndex.Stores.Keys.ToDictionary(key => key, key => mergeSuggestion.MergedIndex.Stores[key]);

                var suggestedSortDict = mergeSuggestion.MergedIndex.SortOptions.Keys.ToDictionary(key => key, key => mergeSuggestion.MergedIndex.SortOptions[key]);

                var expectedSortDict = new Dictionary<string, SortOptions> { { "Customer", SortOptions.String }, { "Email", SortOptions.String } };

   

                bool result = DataDictionaryCompare(suggestedSortDict, expectedSortDict);
                Assert.Equal(true, result);

                Assert.Equal(2, suggestions.Unmergables.Count());

               var unmergebleSuggested =   suggestions.Unmergables;
               var unmergebleExpected = new Dictionary<string, string> {{"Raven/DocumentsByEntityName", "Cannot merge indexes that have a let clause"},{"test3", "Can't find any entity name for merge"}};
               result = DataDictionaryCompare(unmergebleSuggested, unmergebleExpected);
               Assert.Equal(true, result);
            }
        }

        private bool DataDictionaryCompare<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2)
        {
            foreach (var kvp in dataDict1.Keys)
            {
                T v1, v2;
                var found1 = dataDict1.TryGetValue(kvp, out v1);
                var found2 = dataDict2.TryGetValue(kvp, out v2);


                if (found1 && found2 && Equals(v1, v2) == false)
                    return false;

                if(found1 !=found2)
                    return false;


            }
            return true;

        }
    }
}