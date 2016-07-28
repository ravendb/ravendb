using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Indexing.Benchmark.Entities;
using Raven.Client;
using Raven.Client.Indexes;
#if v35
using Raven.Abstractions.Indexing;
#else
using Raven.Client.Indexing;
#endif

namespace Indexing.Benchmark
{
    public class MapIndexesBench : IndexingBenchmark
    {
        private readonly int _numberOfOrdersInDb;

        public MapIndexesBench(IDocumentStore store, int numberOfOrdersInDb) : base(store)
        {
            _numberOfOrdersInDb = numberOfOrdersInDb;
        }

        public override IndexingTestRun[] IndexTestRuns => new []
        {
            new IndexingTestRun
            {
              Index = new Orders_Totals(),
              NumberOfRelevantDocs = _numberOfOrdersInDb
            },
            new IndexingTestRun
            {
              Index = new Orders_ByCompanyNameAndEmploeeFirstName_LoadDocument(),
              NumberOfRelevantDocs = _numberOfOrdersInDb
            },
            new IndexingTestRun
            {
              Index = new Orders_ByProducts_Fanout(),
              NumberOfRelevantDocs = _numberOfOrdersInDb
            }
        };

        public class Orders_Totals : AbstractIndexCreationTask
        {
            public class Result
            {
                public string Employee { get; set; }

                public string Company { get; set; }

                public double Total { get; set; }
            }

            public override string IndexName
            {
                get
                {
                    return "Orders/Totals";
                }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    //                    Maps = { @"from order in docs.Orders
                    //select new { order.Employee,  order.Company, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }" },
                    Maps = { @"from order in docs.Orders
select new { order.Employee,  order.Company, Total = order.Lines.Sum(l =>l.PricePerUnit) }" },
                    //Fields =
                    //{
                    //    {
                    //        "Total", new IndexFieldOptions()
                    //        {
                    //            Sort = SortOptions.NumericDouble
                    //        }
                    //    }
                    //}
                };
            }
        }

        public class Orders_ByCompanyNameAndEmploeeFirstName_LoadDocument : AbstractIndexCreationTask<Order>
        {
            public Orders_ByCompanyNameAndEmploeeFirstName_LoadDocument()
            {
                Map = orders => from o in orders
                                     select new
                                     {
                                         CompanyName = LoadDocument<Company>(o.Company).Name,
                                         EmployeeName = LoadDocument<Employee>(o.Employee).FirstName
                                     };
            }
        }

        public class Orders_ByProducts_Fanout : AbstractIndexCreationTask<Order>
        {
            public Orders_ByProducts_Fanout()
            {
                Map = orders => from o in orders
                    from line in o.Lines
                    select new
                    {
                        line.Product,
                        line.ProductName
                    };
            }
        }

        public class Employees_ByNameAndAddress : AbstractIndexCreationTask<Employee>
        {
            private readonly int _number;

            public Employees_ByNameAndAddress(int number)
            {
                _number = number;

                Map = employees => from e in employees
                    select new
                    {
                        e.FirstName,
                        e.LastName,
                        e.Address
                    };
            }

            public override string IndexName
            {
                get
                {
                    var name = base.IndexName;

                    return $"{name}-{_number}";
                }
            }
        }

        public class Companies_ByNameAndEmail : AbstractIndexCreationTask<Company>
        {
            private readonly int _number;

            public Companies_ByNameAndEmail(int number)
            {
                _number = number;

                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name,
                                       c.Email
                                   };
            }

            public override string IndexName
            {
                get
                {
                    var name = base.IndexName;

                    return $"{name}-{_number}";
                }
            }
        }
    }
}