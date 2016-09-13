// -----------------------------------------------------------------------
//  <copyright file="DictionaryOfEnum.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
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
#pragma warning disable 169,649
            public string Id;
#pragma warning restore 169,649
            public Dictionary<MyEnum, string> Name { get; set; }
        }

        private class Result
        {
#pragma warning disable 169,649
            public string Id;
            public string Name;
#pragma warning restore 169,649
        }

        private class MyTransformer : AbstractTransformerCreationTask<Test>
        {
            public MyTransformer()
            {
                TransformResults = results =>
                    from result in results
                    select new
                    {
                        Name = result.Name.FirstOrDefault(a => a.Key == MyEnum.Value1).Value
                    };
            }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new MyTransformer().Execute(store);
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

                    var myTransformer = s.Load<MyTransformer, Result>("tests/1");
                    Assert.Equal("t", myTransformer.Name);
                }
            }
        }
    }
}
