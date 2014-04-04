using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;
namespace Raven.Tests.Utils
{
    public class DictionaryComparerTest : RavenTest
    {
        public class IndexData
        {
            public void InitializeData1()
            {

                string[] namesArr1 = { "name1", "name2", "name3" };
                FieldStorage[] storesArr1 = { FieldStorage.No, FieldStorage.Yes, FieldStorage.Yes };
                string[] analArr1 = { "anal1", "anal2", "anal3" };
                var sug1 = new SuggestionOptions();
                sug1.Distance = StringDistanceTypes.Levenshtein;
                sug1.Accuracy = (float)0.6;
                var sug2 = new SuggestionOptions();
                var sug3 = new SuggestionOptions();
                sug3.Distance = StringDistanceTypes.Levenshtein;
                sug3.Accuracy = (float)0.45;
                SuggestionOptions[] sugArr1 = { sug1, sug2, sug3 };

   
                FillDictData(namesArr1, storesArr1, analArr1, sugArr1);
  
            }
         public void InitializeData2()
            {

               string[] namesArr2 = { "name2", "name3", "name1" };
                FieldStorage[] storesArr2 = { FieldStorage.Yes, FieldStorage.Yes, FieldStorage.No };
                string[] analArr2 = { "anal2", "anal3", "anal1" };
                var sug21 = new SuggestionOptions();
                sug21.Distance = StringDistanceTypes.Levenshtein;
                sug21.Accuracy = (float)0.6;
                var sug22 = new SuggestionOptions();
                var sug23 = new SuggestionOptions();
                sug23.Distance = StringDistanceTypes.Levenshtein;
                sug23.Accuracy = (float)0.45;
                SuggestionOptions[] sugArr2 = { sug22, sug23, sug21 };

   
   
                FillDictData(namesArr2, storesArr2, analArr2, sugArr2);
  
            }
         public void InitializeData3()
            {
                string[] namesArr3 = { "name1", "name2", "name3" };
                FieldStorage[] storesArr3 = { FieldStorage.No, FieldStorage.Yes, FieldStorage.No };
                string[] analArr3 = { "anal1", "anal2", "anal31" };
                var sug31 = new SuggestionOptions();
                sug31.Distance = StringDistanceTypes.Levenshtein;
                sug31.Accuracy = (float)0.6;
                var sug32 = new SuggestionOptions();
                var sug33 = new SuggestionOptions();
                sug33.Distance = StringDistanceTypes.Levenshtein;
                sug33.Accuracy = (float)0.5;
                SuggestionOptions[] sugArr3 = { sug31, sug32, sug33 };

   
                FillDictData(namesArr3, storesArr3, analArr3, sugArr3);
  
            }
 
            public IDictionary<string, FieldStorage> Stores { get; set; }

            public IDictionary<string, FieldIndexing> Indexes { get; set; }

            public IDictionary<string, SortOptions> SortOptions { get; set; }

            public IDictionary<string, string> Analyzers { get; set; }

            public IList<string> Fields { get; set; }

            public IDictionary<string, SuggestionOptions> Suggestions { get; set; }

            public IDictionary<string, FieldTermVector> TermVectors { get; set; }

            public IDictionary<string, SpatialOptions> SpatialIndexes { get; set; }


            public void FillDictData(string[] namesArr, FieldStorage[] storesArr, string[] analArr, SuggestionOptions[] sugArr)
            {
                for (int i = 0; i < namesArr.Length; i++)
                {
                    Fields.Add(namesArr[i]);

                    Stores.Add(namesArr[i], storesArr[i]);
                    Analyzers.Add(namesArr[i], analArr[i]);
                    Suggestions.Add(namesArr[i], sugArr[i]);


                }

            }
            public void InitDictData()
            {
                Indexes = new Dictionary<string, FieldIndexing>();
                Stores = new Dictionary<string, FieldStorage>();
                Analyzers = new Dictionary<string, string>();
                SortOptions = new Dictionary<string, SortOptions>();
                Suggestions = new Dictionary<string, SuggestionOptions>();
                TermVectors = new Dictionary<string, FieldTermVector>();
                SpatialIndexes = new Dictionary<string, SpatialOptions>();


                Fields = new List<string>();

            }
        }
        public IndexData data1=new IndexData();
        public IndexData data2 = new IndexData();
        public IndexData data3 = new IndexData();

        [Fact]
        public void FullDataCompare()
        {
            data1.InitDictData();
            data2.InitDictData();
            data3.InitDictData();

            data1.InitializeData1();
            data2.InitializeData2();
            data3.InitializeData3();
          bool res = DataDictionaryCompare(data1.Stores.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value),
                        data2.Stores.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value));
 
            Assert.Equal(true, res);

            res = DataDictionaryCompare(data1.Suggestions.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value),
                         data2.Suggestions.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value));
            Assert.Equal(true, res);

            res = DataDictionaryCompare(data1.Analyzers.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value),
               data2.Analyzers.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value));
            Assert.Equal(true, res);

            res = DataDictionaryCompare(data1.Stores.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value),
                        data3.Stores.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value));

            Assert.Equal(false, res);

            res = DataDictionaryCompare(data1.Suggestions.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value),
                         data3.Suggestions.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value));
            Assert.Equal(false, res);

            res = DataDictionaryCompare(data1.Analyzers.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value),
               data3.Analyzers.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value));
            Assert.Equal(false, res);

        }
        private bool DataDictionaryCompare(IDictionary<string, object> dataDict1, IDictionary<string, object> dataDict2)
        {
            return dataDict1.Keys.All(key => dataDict1[key].Equals(dataDict2[key]));
        }

      
    }
}
