using System;
using System.IO;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Server;
using Raven.Tests.Bugs;
using Raven.Tests.Bugs.Caching;
using Raven.Tests.Shard.BlogModel;
using Raven.Tests.Queries;
using Raven.Client.Indexes;
using Raven.Client.Embedded;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tryouts
{
    internal class Program
    {
        private static void Main()
        {
            new IntersectionQuery().CanPeformIntersectionQuery_Embedded();
            new IntersectionQuery().CanPerformIntersectionQuery_Remotely();
        }
    }
}
//namespace FacetedSearch
//{
//    class Program
//    {
//        static void Main(string[] args)
//        {
//            //using (IDocumentStore store = new DocumentStore() { Url = "http://localhost" }.Initialize())
//            using (IDocumentStore store = new EmbeddableDocumentStore() { DataDirectory = @"~\TestingFacetBug" }.Initialize())
//            {
//                CreateFacetSetup(store);

//                AddData(store);

//                QueryFacets(store);

//                Console.Read();
//            }
//        }

//        static void QueryFacets(IDocumentStore store)
//        {
//            using (IDocumentSession session = store.OpenSession())
//            {
//                IndexCreation.CreateIndexes(typeof(CameraFacetsIndex).Assembly, store);

//                session.Query<Camera, CameraFacetsIndex>()
//                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
//                        .ToList();
                
//                var testResult = session.Advanced.LuceneQuery<Camera>("CameraFacetsIndex")
//                                    .Where("Price_Range:[NULL TO Dx200]")
//                                    .ToList();

//                var testResult2 = session.Query<Camera, CameraFacetsIndex>()
//                                    .Where(x => x.Price < 200)                                    
//                                    .ToList();

//                var result = session.Query<Camera, CameraFacetsIndex>()
//                                    .ToFacets("facets/productfacets");

//                foreach (var item in result)
//                {
//                    Console.WriteLine(item.Key);

//                    foreach (var v in item.Value)
//                    {
//                        Console.WriteLine("\t Count:{0} Rango:{1}", v.Count, v.Range);
//                    }
//                }
//            }
//        }

//        static void AddData(IDocumentStore store)
//        {
//            //using (IDocumentStore store = new DocumentStore() { Url = "http://localhost" }.Initialize())
//            using (IDocumentSession session = store.OpenSession())
//            {
//                GetCameras().ForEach((c => session.Store(c)));
//                GetTelevision().ForEach((t => session.Store(t)));

//                session.SaveChanges();
//            }
//        }

//        static List<Camera> GetCameras()
//        {
//            return new List<Camera>()
//            {
//                new Camera {Manufacturer = "Nikkon",Price = 225.2},
//                new Camera {Manufacturer = "Kodak",Price = 235.2},
//                new Camera {Manufacturer = "Nikkon",Price = 1250.2},
//                new Camera {Manufacturer = "XD",Price = 22.2},
//            };
//        }

//        static List<TV> GetTelevision()
//        {
//            return new List<TV>()
//            {
//                new TV {Manufacturer ="Samsung",Price =20},
//                new TV {Manufacturer ="LG",Price =222},
//                new TV {Manufacturer ="Samsung",Price =203},
//                new TV {Manufacturer ="Sony",Price =2032},
//            };
//        }

//        static void CreateFacetSetup(IDocumentStore store)
//        {
//            //using (IDocumentStore store = new DocumentStore() { Url = "http://localhost" }.Initialize())
//            using (IDocumentSession session = store.OpenSession())
//            {
//                var facets = new List<Facet>()
//                {
//                    new Facet {Name = "Manufacturer", Mode = FacetMode.Default},
//                    new Facet {Name= "Price_Range", Ranges = {"[NULL TO Dx200]","[Dx200 TO Dx1000]","[Dx1000 TO NULL]"}, Mode = FacetMode.Ranges}
//                };

//                session.Store(new FacetSetup { Facets = facets, Id = "facets/productfacets" });
//                session.SaveChanges();
//            }
//        }
//    }

//    public class CameraFacetsIndex : AbstractIndexCreationTask<Camera>
//    {
//        public CameraFacetsIndex()
//        {
//            Map = (docs => from doc in docs select new { doc.Manufacturer, doc.Price });
//        }
//    }

//    public class Camera
//    {
//        public string Id { get; set; }
//        public string Manufacturer { get; set; }
//        public string Description { get; set; }
//        public double Price { get; set; }
//    }

//    public class TV
//    {
//        public string Id { get; set; }
//        public string Manufacturer { get; set; }
//        public bool IsFullHD { get; set; }
//        public double Price { get; set; }
//    }
//}