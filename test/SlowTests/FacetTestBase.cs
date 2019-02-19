using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;


namespace SlowTests
{
    public abstract class FacetTestBase : RavenTestBase
    {
        public static void CreateCameraCostIndex(IDocumentStore store)
        {
            var index = new CameraCostIndex();

            store.Maintenance.Send(new PutIndexesOperation(new[] { index.CreateIndexDefinition() }));
        }

        protected class CameraCostIndexStronglyTyped : AbstractIndexCreationTask<Camera>
        {
            public CameraCostIndexStronglyTyped()
            {
                Map = cameras => from camera in cameras
                    select new
                    {
                        camera.Manufacturer,
                        camera.Model,
                        camera.Cost,
                        camera.DateOfListing,
                        camera.Megapixels
                    };
            }

            public override string IndexName => "CameraCost";
        }

        public class CameraCostIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from camera in docs 
                        select new 
                        { 
                            camera.Manufacturer, 
                            camera.Model, 
                            camera.Cost,
                            camera.DateOfListing,
                            camera.Megapixels
                        }"
                    },
                    Name = "CameraCost"
                };
            }

            public override string IndexName => new CameraCostIndex().CreateIndexDefinition().Name;
        }

        protected static void InsertCameraData(IDocumentStore store, IEnumerable<Camera> cameras, bool waitForIndexing = true)
        {
            using (var session = store.OpenSession())
            {
                foreach (var camera in cameras)
                {
                    session.Store(camera);
                }

                session.SaveChanges();
            }

            if (waitForIndexing)
                WaitForIndexing(store);
        }

        public static List<FacetBase> GetFacets()
        {
            return new List<FacetBase>
            {
                new Facet<Camera>
                {
                    FieldName = x => x.Manufacturer
                },
                new RangeFacet<Camera>
                {
                    Ranges =
                    {
                        x => x.Cost <= 200m,
                        x => x.Cost >= 200m && x.Cost <= 400m,
                        x => x.Cost >= 400m && x.Cost <= 600m,
                        x => x.Cost >= 600m && x.Cost <= 800m,
                        x => x.Cost >= 800m
                    }
                },
                new RangeFacet<Camera>
                {
                    Ranges =
                    {
                        x => x.Megapixels <= 3.0m,
                        x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m,
                        x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m,
                        x => x.Megapixels >= 10.0m
                    }
                }
            };
        }

        private static readonly List<string> Features = new List<string>
                    {
                        "Image Stabilizer",
                        "Tripod",
                        "Low Light Compatible",
                        "Fixed Lens",
                        "LCD"
                    };

        private static readonly List<string> Manufacturers = new List<string>
                    {
                        "Sony",
                        "Nikon",
                        "Phillips",
                        "Canon",
                        "Jessops"
                    };

        private static readonly List<string> Models = new List<string>
                    {
                        "Model1",
                        "Model2",
                        "Model3",
                        "Model4",
                        "Model5"
                    };

        private static readonly Random Random = new Random(1337);

        protected static IList<Camera> GetCameras(int numCameras)
        {
            var cameraList = new List<Camera>(numCameras);

            for (int i = 1; i <= numCameras; i++)
            {
                cameraList.Add(new Camera
                {
                    Id = i.ToString(),
                    DateOfListing = new DateTime(1980 + Random.Next(1, 30), Random.Next(1, 12), Random.Next(1, 27)),
                    Manufacturer = Manufacturers[(int)(Random.NextDouble() * Manufacturers.Count)],
                    Model = Models[(int)(Random.NextDouble() * Models.Count)],
                    Cost = (int)(decimal)((Random.NextDouble() * 900.0) + 100.0),    //100.0 to 1000.0
                    Zoom = (int)(Random.NextDouble() * 12) + 2,                 //2.0 to 12.0
                    Megapixels = (decimal)((Random.NextDouble() * 10.0) + 1.0), //1.0 to 11.0
                    ImageStabilizer = Random.NextDouble() > 0.6,
                    AdvancedFeatures = new List<string> { "??" }
                });
            }

            return cameraList;
        }

        protected class Camera
        {
            public string Id { get; set; }

            public DateTime DateOfListing { get; set; }
            public string Manufacturer { get; set; }
            public string Model { get; set; }
            public decimal Cost { get; set; }

            public int Zoom { get; set; }
            public decimal Megapixels { get; set; }
            public bool ImageStabilizer { get; set; }
            public List<String> AdvancedFeatures { get; set; }

            public override string ToString()
            {
                return $"{Id,3}: {DateOfListing} {Manufacturer,10} {Model} - £{Cost:0.00} {Zoom:0.0}X zoom, {Megapixels:0.0} megapixels, [{(AdvancedFeatures == null ? "" : String.Join(", ", AdvancedFeatures))}]";
            }

            public override bool Equals(object obj)
            {
                // If parameter is null return false.
                if (obj == null)
                {
                    return false;
                }

                // If parameter cannot be cast to Point return false.
                var other = obj as Camera;
                if (other == null)
                {
                    return false;
                }

                // Return true if the fields match:
                return Equals(other);
            }

            public bool Equals(Camera other)
            {
                // If parameter is null return false:
                if (other == null)
                {
                    return false;
                }

                const decimal smallValue = 0.00001m;
                // Return true if the fields match:
                return Id == other.Id &&
                       DateOfListing == other.DateOfListing &&
                       Manufacturer == other.Manufacturer &&
                       Model == other.Model &&
                       Math.Abs(Cost - other.Cost) < smallValue &&
                       Zoom == other.Zoom &&
                       Math.Abs(Megapixels - other.Megapixels) < smallValue &&
                       ImageStabilizer == other.ImageStabilizer;
            }

            public override int GetHashCode()
            {
                return (int)(Megapixels * 100) ^ (int)(Cost * 100) ^ (int)DateOfListing.Ticks ^ Id.Length;
            }
        }
    }
}
