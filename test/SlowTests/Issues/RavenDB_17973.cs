using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Xunit.Abstractions;
using TShirt = SlowTests.Core.Utils.Entities.TShirt;
using TShirtType = SlowTests.Core.Utils.Entities.TShirtType;

namespace SlowTests.Issues;

public class RavenDB_17973 : RavenTestBase
{
    public RavenDB_17973(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanPerformIntersectQueryWithFilter()
    {
        using (var store = GetDocumentStore())
        {
            new TShirtIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new TShirt
                {
                    Id = "tshirts/1",
                    Manufacturer = "Raven",
                    ReleaseYear = 2010,
                    Types = new List<TShirtType>
                    {
                        new TShirtType { Color = "Blue", Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Medium" },
                        new TShirtType { Color = "Gray", Size = "Large" }
                    }
                });
                session.Store(new TShirt
                {
                    Id = "tshirts/2",
                    Manufacturer = "Wolf",
                    ReleaseYear = 2011,
                    Types = new List<TShirtType>
                    {
                        new TShirtType { Color = "Blue", Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Large" },
                        new TShirtType { Color = "Gray", Size = "Large" }
                    }
                });
                session.Store(new TShirt
                {
                    Id = "tshirts/3",
                    Manufacturer = "Raven",
                    ReleaseYear = 2011,
                    Types = new List<TShirtType> { new TShirtType { Color = "Yellow", Size = "Small" }, new TShirtType { Color = "Gray", Size = "Large" } }
                });
                session.Store(new TShirt
                {
                    Id = "tshirts/4",
                    Manufacturer = "Raven",
                    ReleaseYear = 2012,
                    Types = new List<TShirtType> { new TShirtType { Color = "Blue", Size = "Small" }, new TShirtType { Color = "Gray", Size = "Large" } }
                });
                session.SaveChanges();
                Indexes.WaitForIndexing(store);

                var tshirtsquery = session.Query<TShirt, TShirtIndex>()
                    .ProjectInto<TShirtIndex.Result>()
                    .Where(x => x.Manufacturer == "Raven")
                    .Intersect()
                    .Where(x => x.Color == "Blue" && x.Size == "Small")
                    .Intersect()
                    .Where(x => x.Color == "Gray" && x.Size == "Large")
                    .OfType<TShirt>()
                    .Filter(p => p.ReleaseYear == 2010)
                    .ProjectInto<TShirtIndex.Result>();

                var tshirts = tshirtsquery.ToArray();

                Assert.Equal(1, tshirts.Length);
                Assert.Equal("tshirts/1", tshirts[0].Id);
            }
        }
    }
}
