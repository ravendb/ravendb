using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Verifications
{
    public class DictionaryOfEnum : RavenTestBase
    {
        private enum MyEnum
        {
            Value1,
            Value2
        }

        private class Test
        {
#pragma warning disable 169, 649
            public string Id;
#pragma warning restore 169, 649
            public Dictionary<MyEnum, string> Name { get; set; }
        }

        private class Result
        {
#pragma warning disable 169, 649
            public string Id;
            public string Name;
#pragma warning restore 169, 649
        }

        private class MyIndex : AbstractIndexCreationTask<Test>
        {
            public MyIndex()
            {
                Map = results =>
                    from result in results
                    select new
                    {
                        Name = result.Name[MyEnum.Value1]
                    };
            }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new MyIndex().Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Store(new Test
                    {
                        Name = new Dictionary<MyEnum, string>
                            {
                                {MyEnum.Value1, "t"},
                                {MyEnum.Value2, "b"}
                            }
                    });
                    s.SaveChanges();
                    WaitForIndexing(store);
                    Assert.Equal(1, s.Query<Result, MyIndex>().Count(x => x.Name == "t"));
                }
            }
        }
    }
}
