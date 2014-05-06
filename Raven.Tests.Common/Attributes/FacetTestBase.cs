using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Tests.Common.Dto.Faceted;

namespace Raven.Tests.Common.Attributes
{
    public abstract class FacetTestBase : RavenTest
    {
        public static void CreateCameraCostIndex(IDocumentStore store)
        {
            var index = new CameraCostIndex();

            store.DatabaseCommands.PutIndex(index.IndexName, index.CreateIndexDefinition());
        }

        public class CameraCostIndex : Raven.Client.Indexes.AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Map =
                        @"from camera in docs 
                        select new 
                        { 
                            camera.Manufacturer, 
                            camera.Model, 
                            camera.Cost,
                            camera.DateOfListing,
                            camera.Megapixels
                        }",
                    Name = "CameraCost"
                };
            }

            public override string IndexName
            {
                get { return new CameraCostIndex().CreateIndexDefinition().Name; }
            }
        }

        public static void InsertCameraDataAndWaitForNonStaleResults(IDocumentStore store, IEnumerable<Camera> cameras)
        {
            using (var session = store.OpenSession())
            {
                foreach (var camera in cameras)
                {
                    session.Store(camera);
                }

                session.SaveChanges();

                session.Query<Camera>(new CameraCostIndex().IndexName)
                    .Customize(x => x.WaitForNonStaleResults())
                    .ToList();
            }
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

        public static IList<Camera> GetCameras(int numCameras)
        {
            var cameraList = new List<Camera>(numCameras);

            for (int i = 1; i <= numCameras; i++)
            {
                cameraList.Add(new Camera
                {
                    Id = i,
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
    }
}
