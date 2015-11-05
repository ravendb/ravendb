using System.Linq;
using Raven.Bundles.Replication.Tasks;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3629 : ReplicationBase
    {

        public class Foo
        {
            public string Id { get; set; }

            public string BarId { get; set; }

            public string Data { get; set; }
        }

        public class Bar
        {
            public string Id { get; set; }

            public string Data { get; set; }
        }

        public class FooBarIndex : AbstractIndexCreationTask<Foo>
        {
            public FooBarIndex()
            {
                Map = docs => from doc in docs
                    let bar = LoadDocument<Bar>(doc.BarId)
                    select new
                    {
                        FooData = doc.Data,
                        BarData = bar.Data
                    };
            }
        }

        [Fact]
        public void Referenced_files_should_be_replicatedA()
        {
            using(var storeA = CreateStore())
            using(var storeB = CreateStore())
            {
                new FooBarIndex().Execute(storeA);
                new FooBarIndex().Execute(storeB);

                using (var session = storeA.OpenSession())
                {
                    session.Store(new Foo
                    {
                        Id = "foo/1",
                        BarId = "bar/1",
                        Data = "FooData"
                    });

                    session.Store(new Bar
                    {
                        Id = "bar/1",
                        Data = "BarData"
                    });

                    session.SaveChanges();
                }

                SetupReplication(storeA.DatabaseCommands, storeB);
                SetupReplication(storeB.DatabaseCommands, storeA);

                WaitForIndexing(storeA);
                WaitForIndexing(storeB);

                using (var session = storeA.OpenSession())
                {
                    var foo = session.Load<Foo>("foo/1");
                    foo.Data = "ChangedFooData";

                    var bar = session.Load<Bar>("bar/1");
                    bar.Data = "ChangedBarData";

                    session.SaveChanges();
                }

                WaitForIndexing(storeA);

                WaitForReplication(storeB, session => session.Load<Foo>("foo/1").Data == "ChangedFooData");
                WaitForReplication(storeB, session => session.Load<Bar>("bar/1").Data == "ChangedBarData");

                using (var session = storeB.OpenSession())
                {
                    var foo = session.Load<Foo>("foo/1");
                    Assert.Equal("ChangedFooData", foo.Data);

                    var bar = session.Load<Bar>("bar/1");
                    Assert.Equal("ChangedBarData", bar.Data);
                }
            }
        }

        [Fact]
        public void Referenced_files_should_be_replicatedB()
        {
            using (var storeA = CreateStore())
            using (var storeB = CreateStore())
            {
                new FooBarIndex().Execute(storeA);
                new FooBarIndex().Execute(storeB);

                using (var session = storeA.OpenSession())
                {
                    session.Store(new Foo
                    {
                        Id = "foo/1",
                        BarId = "bar/1",
                        Data = "FooData"
                    });

                    session.Store(new Bar
                    {
                        Id = "bar/1",
                        Data = "BarData"
                    });

                    session.SaveChanges();
                }

                SetupReplication(storeA.DatabaseCommands, storeB);
                SetupReplication(storeB.DatabaseCommands, storeA);

                WaitForIndexing(storeA);
                WaitForIndexing(storeB);

                using (var session = storeA.OpenSession())
                {
                    var bar = session.Load<Bar>("bar/1");
                    bar.Data = "ChangedBarData";

                    var foo = session.Load<Foo>("foo/1");
                    foo.Data = "ChangedFooData";

                    session.SaveChanges();
                }

                WaitForIndexing(storeA);

                WaitForReplication(storeB, session => session.Load<Foo>("foo/1").Data == "ChangedFooData");
                WaitForReplication(storeB, session => session.Load<Bar>("bar/1").Data == "ChangedBarData");
                using (var session = storeB.OpenSession())
                {
                    var foo = session.Load<Foo>("foo/1");
                    Assert.Equal("ChangedFooData", foo.Data);

                    var bar = session.Load<Bar>("bar/1");
                    Assert.Equal("ChangedBarData", bar.Data);
                }
            }
        }
    }
}
