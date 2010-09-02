using System;
using Raven.Database.Indexing;
using Raven.Client.Tests.Document;
using Raven.Client.Tests.Bugs;
using Lucene.Net.Analysis;
using Raven.Client.Document;
using System.Reflection;
using System.Linq;
using System.IO;
using Raven.Database;
using System.Collections.Generic;
using Raven.Database.Server;
using Newtonsoft.Json.Linq;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
            //var test = new Raven.Tests.Linq.UsingRavenQueryProvider();
            //test.Can_Use_Static_Fields_In_Where_Clauses();

            var test = new Raven.Client.Tests.Document.DocumentStoreServerTests();
            test.Using_attachments_can_properly_set_WebRequest_Headers();

            //DynamicCheckPropertyExistence.Test.RunTest();

            //TestingCustomAnalyzers();

            //TestingAnyQuery();

            //TestingDateTimeBug();
        }
            
        private static void TestingDateTimeBug()
        {
            using (var store = NewDocumentStore())
            {
                //This is a cool feature, see http://ravendb.net/faq/embedded-with-http
                var httpServer = new HttpServer(store.Configuration, store.DocumentDatabase);
                NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
                httpServer.Start();                

                store.DatabaseCommands.PutIndex("DateTime",
                        new IndexDefinition
                        {
                            Map = @"from info in docs.DateTimeInfos                                    
                                    select new { info.TimeOfDay }",
                        });

                var currentTime = DateTime.Now;
                using (var s = store.OpenSession())
                {
                    s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromHours(1) });
                    s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromHours(2) });
                    s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromMinutes(1) });
                    s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromSeconds(10) });
                    
                    s.SaveChanges();
                }

                //Trying to find a fix for the issue here http://github.com/ravendb/ravendb/issues/#issue/145
                using (var s = store.OpenSession())
                {
                    //Just issue a blank query to make sure there are no stale results                    
                    var test = s.Query<DateTimeInfo>("DateTime")                                           
                                .Customize(x => x.WaitForNonStaleResults())                                
                                .Where(x => x.TimeOfDay > currentTime)
                                .ToArray();

                    //try
                    {
                        var testFail = s.Query<DateTimeInfo>("DateTime").Where(x => x.TimeOfDay > DateTime.MinValue); // =====> Throws an exception
                        Console.WriteLine("TestFail Returned {0} items", testFail.Count());
                    }
                    //catch (Exception ex)
                    //{
                    //    Console.WriteLine(ex.Message);
                    //}

                    var dt = DateTime.MinValue;
                    var testPass = s.Query<DateTimeInfo>("DateTime").Where(x => x.TimeOfDay > dt); //=====>Works
                    Console.WriteLine("TestPass Returned {0} items", testPass.Count());
                }

                Console.WriteLine("\nLucene DB available at\n{0}", path);
                Console.WriteLine("Press <ENTER> to close program and delete database");
                Console.ReadLine();

                httpServer.Dispose();
            }



            if (path != null)
                Directory.Delete(path, true);
        }

        //See here http://groups.google.com/group/ravendb/browse_thread/thread/68ad6991f3551230/b8e0657f59bf8d57?lnk=gst&q=dynamic#b8e0657f59bf8d57
        private static void TestingAnyQuery()
        {
            using (var store = NewDocumentStore())
            {
                //This is a cool feature, see http://ravendb.net/faq/embedded-with-http
                var httpServer = new HttpServer(store.Configuration, store.DocumentDatabase);
                NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
                httpServer.Start();

                //Working on issue at http://groups.google.com/group/ravendb/browse_thread/thread/78f1ca6dbdd07e2b
                //The issue only shows up in Server/Client mode, not in Embedded mode!!!
                var key = string.Format("{0}-{1}", "test", DateTime.Now.ToFileTimeUtc()); 
                var metadata = new JObject {
                                       {"owner", 5},
                                       {"Content-Type", "text/plain" },
                                       {"filename", "test.txt" }
                                   };
                store.DatabaseCommands.PutAttachment(key, null, new byte[] { 0, 1, 2 }, metadata);
                Console.WriteLine("Added attachments \"{0}\"", key);

                store.DatabaseCommands.PutIndex("ByLineCost",
                        new IndexDefinition
                        {
                            Map = @"from order in docs.Orders
                                    from line in order.Lines
                                    select new { Cost = line.Cost }",                               
                            //Map = "from order in docs.Orders select new { Cost = order.Test }",  
                        });

                using (var s = store.OpenSession())
                {
                    s.Store(new Order
                    {
                        Lines = new List<OrderItem>
                        {
                            new OrderItem { Cost = 1.59m, Quantity = 5 },
                            new OrderItem { Cost = 7.59m, Quantity = 3 }
                        },
                        Test = 0.5m
                    });
                    s.Store(new Order
                    {
                        Lines = new List<OrderItem>
                        {
                            new OrderItem { Cost = 0.59m, Quantity = 9 },                            
                        },
                        Test = 5.78m
                    });
                   
                    s.SaveChanges();
                }

                //Trying to find a fix for the issue here http://groups.google.com/group/ravendb/browse_thread/thread/68ad6991f3551230
                using (var s = store.OpenSession())
                {
                    //Can't write this cause Order doesn't contain a field called "Cost", it only exists in the index
                    //s.Query<Order>("Orders/ByLineCost")
                    //        .Where(x => x.Cost > 5); 

                    //But this doesn't work also because we an expression tree can't contain a dynamic expression?
                    //See http://stackoverflow.com/questions/2046480/net-4-0-how-to-create-an-expressionfuncdynamic-dynamic-or-is-it-a-bug
                    //s.Query<dynamic>("ByLineCost")
                    //        .Where(x => x.Cost > 5);

                    //We can however do it using a LuceneQuery, but it's really just s.LuceneQuery<object> not s.LuceneQuery<dynamic>
                    //see http://stackoverflow.com/questions/1461801/can-generic-parameters-be-specified-when-using-dynamic-variables/1461848#1461848
                    //var result = s.LuceneQuery<dynamic>("ByLineCost")                                                        
                    //        .Where("Cost_Range:{Dx5 TO NULL}")
                    //        .WaitForNonStaleResults()
                    //        .ToList();

                    //var result = s.QueryDynamic<Order>("ByLineCost")
                    //        .Where(x => x.Cost > 5); 
                    //Console.WriteLine("Dynamic result type = {0}", result);

                    //for issue at http://groups.google.com/group/ravendb/browse_thread/thread/3df57d19d41fc21/fbce0249c711aea6?lnk=gst&q=linq+select#fbce0249c711aea6
                    var selectResult = s.Query<OrderItem>("ByLineCost")
                        .Select(x => new SomeDataProjection { JustCost = x.Cost })
                        .ToArray(); 

                    //Can currently do this
                    s.Query<OrderItem>("ByLineCost").Select(x => x.Cost); //field "Cost" will be fetched

                    try
                    {
                        var test = s.Query<OrderItem>("ByLineCost")
                        //var test = s.Query<Order>("ByLineCost")                            
                                .Customize(x => x.WaitForNonStaleResults())                                
                                .Where(x => x.Cost > 5);
                        Console.WriteLine("Query = \"{0}\"", test.ToString());
                        bool any = test.Any();
                        Console.WriteLine("test has {0} items", test.ToList().Count());
                    }
                    catch (InvalidCastException icEx)
                    {
                        Console.WriteLine(icEx.Message);
                    }
                }

                Console.WriteLine("\nLucene DB available at\n{0}", path);
                Console.WriteLine("Press <ENTER> to close program and delete database");
                Console.ReadLine();

                httpServer.Dispose();
            }

          

            if (path != null)
                Directory.Delete(path, true);
        }

        public class DateTimeInfo
        {
            public string Id { get; set; }
            public DateTime TimeOfDay { get; set; }
        }

        public class SomeDataProjection
        {
            public decimal JustCost { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public decimal Test { get; set; }
            public List<OrderItem> Lines { get; set; }
        }      

        private class OrderItem
        {           
            public decimal Cost { get; set; }
            public decimal Quantity { get; set; }
        }

        private static void TestingCustomAnalyzers()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Movies",
                        new IndexDefinition
                        {
                            Map = "from movie in docs.Movies select new { movie.Name, movie.Tagline, TaglineSimple = movie.Tagline }",
                            Analyzers =
				                {
				                    {"Name", typeof(SimpleAnalyzer).FullName},
				                    {"Tagline", typeof(StopAnalyzer).FullName},
                                    {"TaglineSimple", typeof(SimpleAnalyzer).FullName}
				                }
                        });

                using (var s = store.OpenSession())
                {
                    s.Store(new Movie
                    {
                        Name = "Hello Dolly",
                        Tagline = "She's a jolly good"
                    });
                    s.Store(new Movie
                    {
                        Name = "Star Wars",
                        Tagline = "testing"
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {                   
                    var movies = s.LuceneQuery<Movie>("Movies")
                        .Where("Name:DOLLY")
                        .WaitForNonStaleResults()
                        .ToList();
                    Console.WriteLine(@"There are {0} movies with ""Name:DOLLY""", movies.Count);
                    //Assert.Equal(1, movies.Count);

                    //the Movie class doesn't contain a field call "TaglineSimple" it only exists in the index, 
                    //can we used dynamic to query it using the index "Movie"?
                    //var test = s.Query<dynamic>("Movies")
                    //    .Where(x => x.TaglineSimple == "testing")                        
                    //    .ToList();
                    //Console.WriteLine(@"There are {0} movies with TaglineSimple = ""testing""", test.Count);                    

                    movies = s.LuceneQuery<Movie>("Movies")
                        .Where("Tagline:she's")
                        .WaitForNonStaleResults()
                        .ToList();

                    Console.WriteLine(@"There are {0} movies with ""Tagline:she's""", movies.Count);
                    //Assert.Equal(1, movies.Count);
                }
            }

            Console.WriteLine("\nLucene DB available at\n{0}", path);
            Console.WriteLine("Press <ENTER> to close program and delete database");
            Console.ReadLine();

            if (path != null)
                Directory.Delete(path, true);
        }

        private static string path;
        private static DocumentStore NewDocumentStore()
        {
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);

            if (Directory.Exists(path))
                Directory.Delete(path, true);

            var documentStore = new DocumentStore
            {
                Configuration = new RavenConfiguration
                {
                    DataDirectory = path,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true                    
                }                
            };
            documentStore.Initialize();
            return documentStore;
        }
    }

    public class Movie
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tagline { get; set; }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}