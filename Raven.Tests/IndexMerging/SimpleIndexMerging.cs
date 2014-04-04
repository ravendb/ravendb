// -----------------------------------------------------------------------
//  <copyright file="SimpleIndexMerging.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using System.Text.RegularExpressions;

using Raven.Tests.Common;

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

                var suggestedIndexMap = RemoveSpaces(mergeSuggestion.MergedIndex.Map);
                var expectedIndexMap = "from doc in docs.Orders select new { Customer = doc.Customer, Email = doc.Email }";
                expectedIndexMap = RemoveSpaces(expectedIndexMap);
                    Assert.Equal(expectedIndexMap, suggestedIndexMap);
            }
        }

        [Fact]
        public void WillSuggestMergeTwoSimpleIndexesForSameCollectionWithAdditionalProperties()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test1", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Customer }",
                    Indexes = {{ "Name", FieldIndexing.Analyzed }}
                });
                store.DatabaseCommands.PutIndex("test2", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email }",
                    Indexes = { { "Email", FieldIndexing.Analyzed } }
                });

                var suggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
                Assert.Equal(1, suggestions.Suggestions.Count);

                var mergeSuggestion = suggestions.Suggestions[0];
                Assert.Equal(new[] { "test1", "test2" }, mergeSuggestion.CanMerge);

                var suggestedIndexMap = RemoveSpaces(mergeSuggestion.MergedIndex.Map);
                var expectedIndexMap = "from doc in docs.Orders select new { Customer = doc.Customer, Email = doc.Email }";
                expectedIndexMap = RemoveSpaces(expectedIndexMap);
                Assert.Equal(expectedIndexMap, suggestedIndexMap);
  
                Assert.Equal(FieldIndexing.Analyzed, mergeSuggestion.MergedIndex.Indexes["Name"]);
                Assert.Equal(FieldIndexing.Analyzed, mergeSuggestion.MergedIndex.Indexes["Email"]);
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

                var suggestedIndexMap = RemoveSpaces(mergeSuggestion.MergedIndex.Map);
                var expectedIndexMap = "from doc in docs.Orders select new { Customer = doc.Customer, Email = doc.Email }";
                expectedIndexMap = RemoveSpaces(expectedIndexMap);
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
                    SortOptions = new Dictionary<string, SortOptions> { { "Email", SortOptions.String } }
 
                });

                var suggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
                Assert.Equal(1, suggestions.Suggestions.Count);

                var mergeSuggestion = suggestions.Suggestions[0];
                Assert.Equal(new[] { "test1", "test2", "test3" }, mergeSuggestion.CanMerge);

                var suggestedIndexMap = RemoveSpaces(mergeSuggestion.MergedIndex.Map);
                var expectedIndexMap = "from doc in docs.Orders select new {  Address = doc.Address, Customer = doc.Customer, Email = doc.Email, Tel = doc.Tel }";
                expectedIndexMap = RemoveSpaces(expectedIndexMap);
                Assert.Equal(expectedIndexMap,  suggestedIndexMap);


                var suggestedSortDict = mergeSuggestion.MergedIndex.SortOptions.Keys.ToDictionary(key => key, key => mergeSuggestion.MergedIndex.SortOptions[key]);

                var expectedSortDict = new Dictionary<string, SortOptions> { { "Customer", SortOptions.String }, { "Email", SortOptions.String } };

   

                bool result = DataDictionaryCompare(suggestedSortDict, expectedSortDict);
                Assert.Equal(true, result);

                store.DatabaseCommands.PutIndex("test4", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email,o.Fax }",
                    Stores = new Dictionary<string, FieldStorage> { { "Email", FieldStorage.Yes } },

                });

                var newSuggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
                Assert.Equal(1, newSuggestions.Suggestions.Count);

                var newMergeSuggestion = suggestions.Suggestions[0];
                Assert.Equal(new[] { "test1", "test2", "test3" }, newMergeSuggestion.CanMerge);
                var expectedUnmergebleDict = new Dictionary<string, string> {  {"Raven/DocumentsByEntityName", "Cannot merge indexes that are using a let clause"},{ "test4", "Can't find any other index to merge this with" } };

                bool res = DataDictionaryCompare(suggestions.Unmergables, expectedUnmergebleDict);
                Assert.Equal(true, res);


            }
        }

        [Fact]
        public void UnMergeblesTest()
        {
            //Important : indexes with group bym orderby don't pass syntax check
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test1", new IndexDefinition
                {
                    Map = @"docs.Datas.Where(doc => doc.Info.Contains(""2"")).Select(doc => new {    InfoAndRefernce = doc.Info + ""_"" + ((object) doc.Reference)})",

                });
             
               
              store.DatabaseCommands.PutIndex("test6", new IndexDefinition
              {
                 Map = "from article in docs.Articles select new { CategoryName = article.CategoryName }",                                                        
                 Reduce = "from result in results group result by result.CategoryName into g  select new { CategoryName = g.Key } " 

              });
                store.DatabaseCommands.PutIndex("test7", new IndexDefinition
                {
                    Map = @"from doc in docs let Tag = doc[""metadata""][""Raven-Entity-Name""] select new { Tag, LastModified = (DateTime)doc[""@metadata""][""Modified""] }"
                
                });
                store.DatabaseCommands.PutIndex("test3", new IndexDefinition
                {
                    Map = "from o in docs.Orders select new { o.Email,o.Tel }",
                    SortOptions = new Dictionary<string, SortOptions> { { "Email", SortOptions.String } }

                });
              var suggestions = store.DatabaseCommands.GetIndexMergeSuggestions();
              Assert.Equal(5, suggestions.Unmergables.Count);


                var unmergebleExpectedDict = new Dictionary<string, string>
                {
                    {"Raven/DocumentsByEntityName", "Cannot merge indexes that are using a let clause"},
                    {"test1", "Cannot merge indexes that have a where clause"},
                    {"test6", "Cannot merge map/reduce indexes"},
                    {"test7", "Cannot merge indexes that are using a let clause"},
                    {"test3", "Can't find any other index to merge this with"}


                };
                bool result = DataDictionaryCompare(suggestions.Unmergables, unmergebleExpectedDict);
                Assert.Equal(true, result);


            }
        }

        private string RemoveSpaces(string inputString)
        {
            var resultStr = inputString.Replace("\r\n\t", " ");
            resultStr = resultStr.Replace("\t", " ").Trim();
  
            resultStr = Regex.Replace(resultStr, @"\s+", " ");
            return resultStr;

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