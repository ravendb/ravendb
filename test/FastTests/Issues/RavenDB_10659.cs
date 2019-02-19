using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10659 : RavenTestBase
    {
        private class Element
        {
            public string Name { get; set; }
            public string Value { get; set; }
            public decimal Decimal { get; set; }
        }
        private class Article
        {
            public decimal Value { get; set; }
            public List<Element> Elements { get; set; }
            public Dictionary<string, int> Values { get; set; }
        }

        [Fact]
        public void TranslateDictionaryFunctions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        Value = 1,
                        Elements = new List<Element>()
                        {
                            new Element()
                            {
                                Name = "a",
                                Value = "b",
                                Decimal = 3.2M
                            },
                            new Element()
                            {
                                Name = "a",
                                Value = "b",
                                Decimal = 3.5M
                            }
                        },
                        Values = new Dictionary<string, int>()
                        {
                            ["test"] = 2,
                            ["test1"] = 3,
                            ["test2"] = 4
                        }
                    });

                    session.Store(new Article
                    {
                        Value = 2,
                        Elements = new List<Element>()
                        {
                            new Element()
                            {
                                Name = "aa",
                                Value = "ba",
                                Decimal = 3.5M
                            }
                        },
                        Values = new Dictionary<string, int>()
                        {
                            ["test"] = 1,
                            ["test1"] = 2,
                            ["test2"] = 3
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                let elements = x.Elements.Select(a => a.Decimal)
                                let values = x.Values
                                let generated = x.Elements.ToDictionary(a => a.Name, a => a.Decimal)
                                select new
                                {
                                    TestDictionary1 = values,
                                    TestDictionary2 = x.Values,
                                    TestDictionaryDirectAccess1 = x.Values.Count,
                                    TestDictionaryDirectAccess2 = x.Values.Keys.Cast<string>(),
                                    //TestDictionaryDirectAccess3 = x.Values.Keys, //Should be fixed in raven deserializing of JSON
                                    TestDictionaryDirectAccess4 = x.Values.Values.Cast<int>(),
                                    //TestDictionaryDirectAccess5 = x.Values.Values, //Should be fixed in raven deserializing of JSON
                                    TestDictionarySum1 = values.Sum(a => a.Value),
                                    TestDictionarySum2 = x.Values.Sum(a => a.Value),
                                    TestDictionarySum3 = x.Values.Values.Sum(),
                                    TestDictionaryAverage1 = values.Average(a => a.Value),
                                    TestDictionaryAverage2 = x.Values.Average(a => a.Value),
                                    TestDictionaryAverage3 = x.Values.Values.Average(),
                                    
                                    TestDictionaryFunc1 = x.Values.Count(),
                                    TestDictionaryFunc2 = x.Values.Select(a => a.Value),


                                    TestGeneratedDictionary1 = generated,
                                    TestGeneratedDictionary2 = x.Elements.ToDictionary(a => a.Name, a => a.Decimal),
                                    TestGeneratedDictionary3 = generated.Count(),
                                    TestGeneratedDictionarySum1 = generated.Sum(a => a.Value),
                                    TestGeneratedDictionarySum2 = x.Elements.ToDictionary(a => a.Name, a => a.Decimal).Sum(a => a.Value),  // JS: ToDictionary -> ToKeyValuePair -> Sum
                                    TestGeneratedDictionaryAverage1 = generated.Average(a => a.Value),
                                    TestGeneratedDictionaryAverage2 = x.Elements.ToDictionary(a => a.Name, a => a.Decimal).Average(a => a.Value),
                                    TestGeneratedDictionaryDirectAccess1 = generated.Keys.ToList(),
                                    TestGeneratedDictionaryDirectAccess2 = generated.Values.ToList(),
                                    TestGeneratedDictionaryDirectAccess3 = generated.Count,

                                    TestList1 = elements.Sum(),
                                    TestList2 = x.Elements.Sum(a => a.Decimal),
                                    TestList3 = x.Elements.Select(a => a.Decimal).Sum(),
                                    TestList4 = x.Elements.Average(a => a.Decimal),
                                    TestList5 = x.Elements.Select(a => a.Decimal).Average()
                                };

                    //Assert.Equal("from Articles as x select { Round : Math.round(x.Value), Round2 : Math.round(x.Value * Math.pow(10, 2)) / Math.pow(10, 2), Round4 : Math.round(x.Value * Math.pow(10, 4)) / Math.pow(10, 4) }", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(3, result[0].TestDictionary1.Count);
                    Assert.Equal(3, result[0].TestDictionary2.Count);
                    Assert.Equal(3, result[0].TestDictionaryDirectAccess1);
                    Assert.Equal(new[] { "test", "test1", "test2" }, result[0].TestDictionaryDirectAccess2);
                    Assert.Equal(new[] { 2,3,4 }, result[0].TestDictionaryDirectAccess4);

                    Assert.Equal(9, result[0].TestDictionarySum1);
                    Assert.Equal(9, result[0].TestDictionarySum2);
                    Assert.Equal(9, result[0].TestDictionarySum3);

                    Assert.Equal(3, result[0].TestDictionaryAverage1);
                    Assert.Equal(3, result[0].TestDictionaryAverage2);
                    Assert.Equal(3, result[0].TestDictionaryAverage3);

                    Assert.Equal(3, result[0].TestDictionaryFunc1);
                    Assert.Equal(new[] { 2, 3, 4 }, result[0].TestDictionaryFunc2);

                    Assert.Equal(1, result[0].TestGeneratedDictionary1.Count);
                    Assert.Equal(1, result[0].TestGeneratedDictionary2.Count);
                    Assert.Equal(1, result[0].TestGeneratedDictionary3);

                    Assert.Equal(3.5M, result[0].TestGeneratedDictionarySum1);
                    Assert.Equal(3.5M, result[0].TestGeneratedDictionarySum2);
                    Assert.Equal(3.5M, result[0].TestGeneratedDictionaryAverage1);
                    Assert.Equal(3.5M, result[0].TestGeneratedDictionaryAverage2);

                    Assert.Equal(new[] { "a" }, result[0].TestGeneratedDictionaryDirectAccess1);
                    Assert.Equal(new[] { 3.5M }, result[0].TestGeneratedDictionaryDirectAccess2);
                    Assert.Equal(1, result[0].TestGeneratedDictionaryDirectAccess3);


                    Assert.Equal(6.7M, result[0].TestList1);
                    Assert.Equal(6.7M, result[0].TestList2);
                    Assert.Equal(6.7M, result[0].TestList3);
                    Assert.Equal(3.35M, result[0].TestList4);
                    Assert.Equal(3.35M, result[0].TestList5);

                }
            }
        }

    }
}
