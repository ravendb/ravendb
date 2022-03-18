using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.MultiMap
{
    public class MultiMapWithNullableEnumAndCoalescingOperator : RavenTestBase
    {
        public MultiMapWithNullableEnumAndCoalescingOperator(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_create_index()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Obj1 { Name = "Tom", MyEnumField = MyEnum.OtherValue });
                    session.Store(new Obj1 { Name = "Oscar" });

                    session.SaveChanges();
                }

                new MySearchIndexTask().Execute(store);

                Indexes.WaitForIndexing(store);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<Obj1, MySearchIndexTask>()
                                        .Where(x => x.MyEnumField == MyEnum.OtherValue)
                                        .ToList());

                    Assert.NotEmpty(s.Query<Obj1, MySearchIndexTask>()
                                        .Where(x => x.Name == "Oscar")
                                        .ToList());

                }
            }
        }

        private enum MyEnum
        {
            Default = 0,
            OtherValue = 1,
            YetAnotherValue = 2
        }

        private class Tag
        {
            public string Name { get; set; }
        }

        private class Obj1
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public MyEnum? MyEnumField { get; set; }
            public Tag[] Tags { get; set; }
            public bool IsDeleted { get; set; }
        }

        private class MySearchIndexTask : AbstractMultiMapIndexCreationTask<MySearchIndexTask.Result>
        {
            public class Result
            {
                public object[] Content { get; set; }
                public string Name { get; set; }
                public MyEnum MyEnumField { get; set; }
            }

            public override string IndexName { get { return "MySearchIndexTask"; } }
            public MySearchIndexTask()
            {
                AddMap<Obj1>(items => from item in items
                                      where item.IsDeleted == false
                                      select new Result
                                      {
                                          Name = item.Name,
                                          Content = new object[] { item.Name.Boost(3), item.Tags.Select(x => x.Name).Boost(2), item.Description },
                                          MyEnumField = item.MyEnumField ?? MyEnum.Default
                                      });

                Index(x => x.Content, FieldIndexing.Search);
                Index(x => x.Name, FieldIndexing.Default);
                Index(x => x.MyEnumField, FieldIndexing.Default);
            }
        }

    }
}
