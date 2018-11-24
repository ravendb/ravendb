using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12372 : RavenTestBase
    {
        private class Test
        {
            public string Name { get; set; }
            public Dictionary<string, string> NameTranslations { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<Test>
        {
            public TestIndex()
            {
                Map = tests => from test in tests
                               select new
                               {
                                   test.Name,
                                   _ = test.NameTranslations
                                   .Select(kv =>
                                     CreateField("NameTranslations_" + kv.Key, kv.Value, new CreateFieldOptions
                                     {
                                         Indexing = FieldIndexing.Search
                                     }))
                               };

                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        [Fact]
        public void TestSearch()
        {
            using (var store = GetDocumentStore())
            {
                var d = new Test
                {
                    Name = "test translation",
                    NameTranslations = new Dictionary<string, string>
                    {
                        {"en", "test translation"}
                    }
                };

                new TestIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(d);
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var result = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("Name", "test").ToList().Count;

                    var result1 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("Name", "translation").ToList().Count;

                    var result2 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("Name", "tes*").ToList().Count;

                    var result3 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("Name", "translat*").ToList().Count;

                    var result4 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("Name", "test translation").ToList().Count;

                    var result5 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("NameTranslations_en", "test").ToList().Count;

                    var result6 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("NameTranslations_en", "translation").ToList().Count;

                    var result7 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("NameTranslations_en", "tes*").ToList().Count;

                    var result8 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("NameTranslations_en", "translat*").ToList().Count;

                    var result9 = session.Advanced.DocumentQuery<Test, TestIndex>()
                        .Search("NameTranslations_en", "test translation").ToList().Count;

                    Assert.Equal(result, 1);
                    Assert.Equal(result1, 1);
                    Assert.Equal(result2, 1);
                    Assert.Equal(result3, 1);
                    Assert.Equal(result4, 1);
                    Assert.Equal(result5, 1);
                    Assert.Equal(result6, 1);
                    Assert.Equal(result7, 1);
                    Assert.Equal(result8, 1);
                    Assert.Equal(result9, 1);
                }
            }
        }
    }
}
