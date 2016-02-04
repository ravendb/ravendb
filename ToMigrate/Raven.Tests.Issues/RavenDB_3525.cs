// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3525.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3525 : RavenTest
    {
        [Fact]
        public void WithinRadiusOf_NamedSpatialField()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentStore.ExecuteIndex(new NamedSpatialFieldIndex());

                    var obj = new ClassWithLocation
                    {
                        Id = "obj/1",
                        Location = new Location(10.0, 11.0)
                    };

                    session.Store(obj);
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var namedLuceneQuery = session.Advanced
                        .LuceneQuery<ClassWithLocation, NamedSpatialFieldIndex>()
                        .WithinRadiusOf("Location", 1.0, 10.0, 11.0)
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Single(namedLuceneQuery);

                    var linqQueryWithSpatial = session.Query<ClassWithLocation, NamedSpatialFieldIndex>()
                        .Spatial(x => x.Location, x => x.WithinRadiusOf(1.0, 11.0, 10.0))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Single(linqQueryWithSpatial); // for some reason this doesn't work

                    linqQueryWithSpatial = session.Query<ClassWithLocation, NamedSpatialFieldIndex>()
                        .Spatial(x => x.Location, x => x.WithinRadius(1.0, 10.0, 11.0))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Single(linqQueryWithSpatial); // for some reason this doesn't work

                    var linqQueryWithCustomize = session.Query<ClassWithLocation, NamedSpatialFieldIndex>()
                        .Customize(x => x.WithinRadiusOf("Location", 1.0, 10.0, 11.0))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Single(linqQueryWithCustomize); // this works
                }
            }
        }

        private class ClassWithLocation
        {
            public string Id { get; set; }
            public Location Location { get; set; }
        }

        private class ClassWithLocationReduceResult
        {
            public string Id { get; set; }
            public Location Location { get; set; }
        }

        private class Location
        {
            public Location(double latitude, double longitude)
            {
                Latitude = latitude;
                Longitutde = longitude;
            }

            public double Latitude { get; set; }

            public double Longitutde { get; set; }
        }

        private class NamedSpatialFieldIndex : AbstractIndexCreationTask<ClassWithLocation, ClassWithLocationReduceResult>
        {
            public NamedSpatialFieldIndex()
            {
                Map = docs => from d in docs
                              select new ClassWithLocationReduceResult()
                              {
                                  Id = d.Id,
                                  Location = (Location)SpatialGenerate("Location", d.Location.Latitude, d.Location.Longitutde)
                              };
            }
        }
    }
}
