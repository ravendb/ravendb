using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using RavenView.Views;

namespace RavenView
{
    class Program
    {
        static EmbeddableDocumentStore store; 
        static void Main(string[] args)
        {
            CreateStore();
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            AddRecord(1);
            WaitIndexes();
            ReadRecords(600);
            Console.ReadKey();

            //var recs = 600;

            //do
            //{
            //    AddRecord(1);
            //    AddRecord(1);
            //    WaitIndexes();
            //    recs += 100;
            //    ReadRecords(recs);
                
            //} while (Console.ReadKey().Key != ConsoleKey.Enter);
    
            //Console.ReadKey();
        }

        private static void WaitIndexes()
        {
            
            do
            {
                Thread.Sleep(50);

            } while(store.DocumentDatabase.Statistics.StaleIndexes.Length > 0) ;
        }

        private static int ReadRecords(int shouldBe)
        {
            var session = store.OpenSession();
            int count = session.Advanced.LuceneQuery<object>("view1").WaitForNonStaleResults().QueryResult.TotalResults;
            Console.WriteLine("View1 {0} {1}", count, count == shouldBe);
            count = session.Advanced.LuceneQuery<object>("view2").WaitForNonStaleResults().QueryResult.TotalResults;
            Console.WriteLine("View2 {0} {1}", count, count == shouldBe);
            count = session.Advanced.LuceneQuery<object>("view3").WaitForNonStaleResults().QueryResult.TotalResults;
            Console.WriteLine("View3 {0} {1}", count, count == shouldBe);
            count = session.Advanced.LuceneQuery<object>("view4").WaitForNonStaleResults().QueryResult.TotalResults;
            Console.WriteLine("View4 {0} {1}", count, count == shouldBe);
            count = session.Advanced.LuceneQuery<object>("view5").WaitForNonStaleResults().QueryResult.TotalResults;;
            Console.WriteLine("View5 {0} {1}", count, count == shouldBe);
            count = session.Advanced.LuceneQuery<object>("view6").WaitForNonStaleResults().QueryResult.TotalResults;
            Console.WriteLine("View6 {0} {1}", count, count == shouldBe);
            session.Dispose();
            return count;
        }

        private static void AddRecord(int records)
        {
            var session = store.OpenSession();
            for (int i = 0; i < records; i++)
            {
                var item = new TestClass();
                item.Items = new List<Item>();
                for (int j = 0; j < 50; j++)
                {
                    item.Items.Add(new Item() {Id = j+1, Email = string.Format("rob{0}@text.com", i+1).PadLeft(200, (char)i), Name = string.Format("rob{0}", i+1).PadLeft(300,(char)i)});    
                }
                session.Store(item);
            }
            session.SaveChanges();
            session.Dispose();
        }

        private static void CreateStore()
        {
            store = new EmbeddableDocumentStore
                            {
                                Conventions = Conventions.Document,
                                Configuration =
                                    {
                                        RunInMemory = true
                                    },
                                
                            };
            store.Configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(TestClassView).Assembly));
            store.Initialize();
            
        }
    }
}
