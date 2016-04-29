using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using FastTests;

using Raven.Client;

using SlowTests.Core.Utils.Indexes;

using Xunit;

using TShirt = SlowTests.Core.Utils.Entities.TShirt;
using TShirtType = SlowTests.Core.Utils.Entities.TShirtType;

namespace SlowTests.Core.Querying
{
    public class Intersection : RavenTestBase
    {
        [Fact(Skip = "Missing feature: Static indexes - TODO [ppekrol]")]
        public async Task CanPerformIntersectQuery()
        {
            using (var store = await GetDocumentStore())
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
                            new TShirtType {Color = "Blue", Size = "Small"},
                            new TShirtType {Color = "Black", Size = "Small"},
                            new TShirtType {Color = "Black", Size = "Medium"},
                            new TShirtType {Color = "Gray", Size = "Large"}
                        }
                    });
                    session.Store(new TShirt
                    {
                        Id = "tshirts/2",
                        Manufacturer = "Wolf",
                        ReleaseYear = 2011,
                        Types = new List<TShirtType>
                        {
                            new TShirtType { Color = "Blue",  Size = "Small" },
                            new TShirtType { Color = "Black", Size = "Large" },
                            new TShirtType { Color = "Gray",  Size = "Large" }
                        }
                    });
                    session.Store(new TShirt
                    {
                        Id = "tshirts/3",
                        Manufacturer = "Raven",
                        ReleaseYear = 2011,
                        Types = new List<TShirtType>
                        {
                            new TShirtType { Color = "Yellow",  Size = "Small" },
                            new TShirtType { Color = "Gray",  Size = "Large" }
                        }
                    });
                    session.Store(new TShirt
                    {
                        Id = "tshirts/4",
                        Manufacturer = "Raven",
                        ReleaseYear = 2012,
                        Types = new List<TShirtType>
                        {
                            new TShirtType { Color = "Blue",  Size = "Small" },
                            new TShirtType { Color = "Gray",  Size = "Large" }
                        }
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var tshirts = session.Query<TShirt, TShirtIndex>()
                        .ProjectFromIndexFieldsInto<TShirtIndex.Result>()
                        .Where(x => x.Manufacturer == "Raven")
                        .Intersect()
                        .Where(x => x.Color == "Blue" && x.Size == "Small")
                        .Intersect()
                        .Where(x => x.Color == "Gray" && x.Size == "Large")
                        .ToArray();

                    Assert.Equal(2, tshirts.Length);
                    Assert.Equal("tshirts/1", tshirts[0].Id);
                    Assert.Equal("tshirts/4", tshirts[1].Id);
                }
            }
        }
    }
}
