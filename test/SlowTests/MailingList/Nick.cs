using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.MailingList
{
    public class Nick : RavenTestBase
    {
        [Flags]
        private enum MyEnum
        {
            None = 0,
            First = 1,
            Second = 2
        }

        private class Entity
        {
            public string Id { set; get; }
            public string Name { set; get; }
            public MyEnum Status { set; get; }
        }

        private class MyIndex : AbstractIndexCreationTask<Entity, MyIndex.Result>
        {
            public class Result
            {
#pragma warning disable 649,169
                public bool IsFirst;
                public bool IsSecond;
#pragma warning restore 649,169
            }

            public MyIndex()
            {
                Map = entities => from entity in entities
                                  select new
                                  {
                                      IsFirst = (entity.Status & MyEnum.First) == MyEnum.First,
                                      IsSecond = (entity.Status & MyEnum.Second) == MyEnum.Second
                                  };
            }
        }

        [Fact]
        public void CanQueryUsingBitwiseOperations()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.SaveEnumsAsIntegers = true;

                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var entity = new Entity
                    {
                        Name = "name1",
                        Status = MyEnum.First | MyEnum.Second
                    };
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyIndex.Result, MyIndex>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                        .Where(x => x.IsSecond)
                        .As<Entity>()
                        .ToList();

                    TestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(results);
                }
            }
        }
    }
}
