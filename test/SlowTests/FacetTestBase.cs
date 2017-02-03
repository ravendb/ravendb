using System;
using System.Collections.Generic;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;


namespace SlowTests
{
    public abstract class FacetTestBase : RavenNewTestBase
    {
        public static void CreateCameraCostIndex(IDocumentStore store)
        {
            var index = new CameraCostIndex();

            store.Admin.Send(new PutIndexOperation(index.IndexName, index.CreateIndexDefinition()));
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

        public static List<Facet> GetFacets()
        {
            return new List<Facet>
            {
                new Facet<Camera> {Name = x => x.Manufacturer},
                new Facet<Camera>
                {
                    Name = x => x.Cost,
                    Ranges =
                        {
                            x => x.Cost < 200m,
                            x => x.Cost > 200m && x.Cost < 400m,
                            x => x.Cost > 400m && x.Cost < 600m,
                            x => x.Cost > 600m && x.Cost < 800m,
                            x => x.Cost > 800m
                        }
                },
                new Facet<Camera>
                {
                    Name = x => x.Megapixels,
                    Ranges =
                        {
                            x => x.Megapixels < 3.0m,
                            x => x.Megapixels > 3.0m && x.Megapixels < 7.0m,
                            x => x.Megapixels > 7.0m && x.Megapixels < 10.0m,
                            x => x.Megapixels > 10.0m
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

        private static readonly Random random = new Random(1337);

        protected static IList<Camera> GetCameras(int numCameras)
        {
            var cameraList = new List<Camera>(numCameras);

            for (int i = 1; i <= numCameras; i++)
            {
                cameraList.Add(new Camera
                {
                    Id = i.ToString(),
                    DateOfListing = new DateTime(1980 + random.Next(1, 30), random.Next(1, 12), random.Next(1, 27)),
                    Manufacturer = Manufacturers[(int)(random.NextDouble() * Manufacturers.Count)],
                    Model = Models[(int)(random.NextDouble() * Models.Count)],
                    Cost = (int)(decimal)((random.NextDouble() * 900.0) + 100.0),    //100.0 to 1000.0
                    Zoom = (int)(random.NextDouble() * 12) + 2,                 //2.0 to 12.0
                    Megapixels = (decimal)((random.NextDouble() * 10.0) + 1.0), //1.0 to 11.0
                    ImageStabilizer = random.NextDouble() > 0.6,
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