using System.Data;
using System.Globalization;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Raven.Abstractions.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;



namespace Raven.Tests.Issues
{
    public class RavenDb_1816 : RavenTest
    {
       const string DbName = "TestDb";
       private const int Quantity = 1000;

       

        [Fact]
        public void CanLoadLongQuerry()
        {
            FillLaptopDb();
            var list = Enumerable.Range(1, 1000).ToList();
            using (var store = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = DbName }.Initialize())
            {
                new LaptopIndex().Execute(store);
                using (var session = store.OpenSession(DbName))
                {
                     var q = session.Query<Laptop,LaptopIndex>()
                     .Where(x => x.Id.In(list));

                     WaitForIndexing(store);

                     //QueryHeaderInformation queryHeaders;
                   
                     //var enumerator = store.DatabaseCommands.StreamQuery(new LaptopIndex().IndexName, new IndexQuery
                     //{
                     //    Query = q.ToString()
                     //}, out queryHeaders);
                     //Assert.Equal(1, queryHeaders.TotalResults);

                    using (var streamingQuery = session.Advanced.Stream(q))
                    {
                        var streamingResults = new List<Laptop>();
                        while (streamingQuery.MoveNext())
                            streamingResults.Add(streamingQuery.Current.Document);

                        Console.WriteLine("Streaming results count: " + streamingResults.Count);
                        Assert.Equal(streamingResults.Count,Quantity);
                    }



                  

                }
            }
        }

        public void FillLaptopDb()
        {
             using (var store = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = DbName }.Initialize())
             {
                 var bulk_options = new BulkInsertOptions
                 {
                     OverwriteExisting = true,
                     BatchSize = 100
                 };
                using (var bulkInsert = store.BulkInsert(DbName,bulk_options))
                    {
                        for (int cntr =1 ;cntr <=Quantity; cntr++)
                        {
                             bulkInsert.Store(new Laptop {
                            Id = cntr,
                            Cpu = "Pent" + cntr,
                            Manufacturer = "Intel",
                            HDDSizeInGigabytes = (100000 + cntr).ToString(CultureInfo.InvariantCulture),
                            RamSizeInMegabatye = (10 + cntr).ToString(CultureInfo.InvariantCulture),
 
                             });
                        }
                    }
                   
               
                
            }
        }
    }
    public class LaptopIndex : AbstractIndexCreationTask<Laptop>
    {
        //public LaptopIndex()
        //{
        //    Map = laptops => from laptop in laptops
        //                     select new
        //                     {
        //                         laptop.Id,
        //                         laptop.Cpu,
        //                         laptop.Manufacturer,
        //                         laptop.HDDSizeInGigabytes,
        //                         laptop.RamSizeInMegabatye
        //                     };
        //}
        public LaptopIndex()
        {
            Map = laptops => from laptop in laptops
                             select new
                             {
                                 laptop.Id

                             };
        }
    }
    public class Laptop
    {
        public int Id { get; set; }
        public string Cpu { get; set; }
        public string Manufacturer { get; set; }
        public string HDDSizeInGigabytes { get; set; }
        public string RamSizeInMegabatye { get; set; }
    }
   
   
    }

   
    
