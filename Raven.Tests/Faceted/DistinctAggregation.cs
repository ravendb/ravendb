using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Common.Dto.Faceted;
using Raven.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Faceted
{
    public class DistinctAggregation : FacetTestBase
    {
        [Fact]
        public async Task GetDistinctValuesForRegulatFacet()
        {
            var cameras = GetCameras(1000);
            
            cameras.Add(new Camera
            {
                Id = cameras.Count +1,
                Manufacturer = "Zenit",
                Model = "Zenit 12"
            });
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", Aggregation = FacetAggregation.Distinct, AggregationField = "Manufacturer"  }, new Facet { Name = "Model", Aggregation = FacetAggregation.Distinct, AggregationField = "Model" } };
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                new CameraCostIndex().Execute(store);

                using (var session = store.OpenAsyncSession())
                {
                    foreach (var camera in cameras)
                    {
                        await session.StoreAsync(camera);
                    }
                    await session.StoreAsync(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var res = await session.Query<Camera>("CameraCost").Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Manufacturer == "Zenit").ToListAsync();
                    var facetResults = await session.Query<Camera>("CameraCost")
                        .Where(x => x.Manufacturer == "Zenit")
                        .ToFacetsAsync("facets/CameraFacets");

                    Assert.True(facetResults.Results["Manufacturer"].Values.Any(x => x.Range.Equals("Zenit", StringComparison.InvariantCultureIgnoreCase)));
                    Assert.True(facetResults.Results["Model"].Values.Any(x => x.Range.Equals("Zenit 12", StringComparison.InvariantCultureIgnoreCase)));
                }


                using (var session = store.OpenAsyncSession())
                {
                    var res = await session.Query<Camera>("CameraCost").Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Manufacturer != "Zenit").ToListAsync();
                    var facetResults = await session.Query<Camera>("CameraCost")
                        .Where(x=>x.Manufacturer != "Zenit")
                        .ToFacetsAsync("facets/CameraFacets");

                    Assert.False(facetResults.Results["Manufacturer"].Values.Any(x => x.Range.Equals("Zenit", StringComparison.InvariantCultureIgnoreCase)));
                    Assert.False(facetResults.Results["Model"].Values.Any(x => x.Range.Equals("Zenit 12", StringComparison.InvariantCultureIgnoreCase)));
                }
            }
        }


        [Fact]
        public async Task GetDistinctValuesForRangeFacets()
        {
            var cameras = GetCameras(1000);

            cameras.Add(new Camera
            {
                Id = cameras.Count + 1,
                Cost = 15000,
                Megapixels = 50,
                Zoom = 50                
            });
            var facets = new List<Facet> {
                new Facet {
                    Mode = FacetMode.Ranges,
                    Name = "Cost_Range",
                    Aggregation = FacetAggregation.Distinct,
                    AggregationType = "int",
                    AggregationField = "Cost_Range" ,
                    Ranges = new List<string>{ "[Ix0 TO Ix1000]", "[Ix10000 TO Ix20000]" }
                },
                new Facet {
                    Mode = FacetMode.Ranges,
                    Name = "Megapixels_Range",
                    Aggregation = FacetAggregation.Distinct,
                    AggregationType = "double",
                    AggregationField = "Megapixels_Range",                    
                    Ranges = new List<string>{ "[Dx1.0 TO Dx12.0]", "[Dx44 TO Dx55]" }
                }
            };

            using (var store = NewRemoteDocumentStore(fiddler: true))
            {
                new CameraCostIndex().Execute(store);

                using (var session = store.OpenAsyncSession())
                {
                    foreach (var camera in cameras)
                    {
                        await session.StoreAsync(camera);
                    }
                    await session.StoreAsync(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var res = await session.Query<Camera>("CameraCost")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Cost == 15000).ToListAsync();
                    var facetResults = await session.Query<Camera>("CameraCost")
                        .Where(x => x.Cost == 15000)
                        .ToFacetsAsync("facets/CameraFacets");

                    var costs = facetResults.Results["Cost_Range"].Values;
                    // "0 To 100",  "10000 To 20000"
                    /*
                     Ranges = new List<string>{ "[Ix0 TO Ix1000]", "[Ix10000 TO Ix20000]" }
                    Ranges = new List<string>{ "[Dx1.0 TO Dx12.0]", "[Dx44 TO Dx55]" }
                     */
                    Assert.True(costs.First(x => x.Range.Equals("[Ix10000 TO Ix20000]", StringComparison.InvariantCultureIgnoreCase)).Exists);
                    Assert.False(costs.First(x => x.Range.Equals("[Ix0 TO Ix1000]", StringComparison.InvariantCultureIgnoreCase)).Exists);

                    
                    var megapixels = facetResults.Results["Megapixels_Range"].Values;
                    Assert.True(megapixels.First(x => x.Range.Equals("[Dx44 TO Dx55]", StringComparison.InvariantCultureIgnoreCase)).Exists);
                    Assert.False(megapixels.First(x => x.Range.Equals("[Dx1.0 TO Dx12.0]", StringComparison.InvariantCultureIgnoreCase)).Exists);                    
                }


                using (var session = store.OpenAsyncSession())
                {
                    var res = await session.Query<Camera>("CameraCost")
                        .Where(x => x.Cost != 15000).ToListAsync();
                    var facetResults = await session.Query<Camera>("CameraCost")
                        .Where(x => x.Cost != 15000)
                        .ToFacetsAsync("facets/CameraFacets");

                    var costs = facetResults.Results["Cost_Range"].Values;
                    // "0 To 100",  "10000 To 20000"
                    Assert.False(costs.First(x => x.Range.Equals("[Ix10000 TO Ix20000]", StringComparison.InvariantCultureIgnoreCase)).Exists);
                    Assert.True(costs.First(x => x.Range.Equals("[Ix0 TO Ix1000]", StringComparison.InvariantCultureIgnoreCase)).Exists);


                    var megapixels = facetResults.Results["Megapixels_Range"].Values;
                    Assert.False(megapixels.First(x => x.Range.Equals("[Dx44 TO Dx55]", StringComparison.InvariantCultureIgnoreCase)).Exists);
                    Assert.True(megapixels.First(x => x.Range.Equals("[Dx1.0 TO Dx12.0]", StringComparison.InvariantCultureIgnoreCase)).Exists);
                }
            }
        }
    }
}
