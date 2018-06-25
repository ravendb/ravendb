using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using SlowTests.Tests.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDBBookJavaScriptIndexes : RavenTestBase
    {

        [Fact]
        public void PlaygroundTest()
        {
            
            ExecuteIndexAndWaitForUserToContinueTheTest<RevenuePerProduct>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<DynamicFields>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeAndManager>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<SpatialIndex>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<ProductsSearch>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<OrdersSearch>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<MultiField>();            
            //ExecuteIndexAndWaitForUserToContinueTheTest<MultiMap>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<OrdersTotal>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeByName>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeByHiredAt>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<EmployeeIndex>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<RegionsByTerritoriesName>();
        }

        public void ExecuteIndexAndWaitForUserToContinueTheTest<T>() where T : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                new T().Execute(store);
                WaitForUserToContinueTheTest(store);
            }
        }

        private void CreateNorthwindDatabase(DocumentStore store)
        {
            store.Maintenance.Send(new CreateSampleDataOperation());
        }

        private class DynamicFields : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "DynamicFields",
                    Maps = new HashSet<string>
                    {
                        @"map('Employees', function (e){ 
    var multipleDynamicFields = [];
    var o = e.Address;
    for (var p in o) {      
      multipleDynamicFields.push(
                    {
                        $value: o[p], 
                        $name: p, 
                        $options:{index: true, store: true}
                    })
    }
    return { 
             _: multipleDynamicFields,
             __:  {$value: e.LastName, $name: e.FirstName, $options: {index: true, store: true} } ,
            Name: e.FirstName + ' ' + e.LastName
           };                            
})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }

        }

        private class EmployeeAndManager : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeAndManager",
                    Maps = new HashSet<string>
                        {
                            @"map('Employees', function (e){ 
    var m = load(e.ReportsTo , 'Employees');
    return { 
             FirstName: e.FirstName  ,
             LastName: e.LastName,
             ManagerFirstName: m.FirstName,
             ManagerLastName: m.LastName
           };                            
})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class SpatialIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "SpatialIndex",
                    Maps = new HashSet<string>
                        {
                            @"map('Companies', function (c){ 
    return { 
             Name: c.Name  ,
             Coordinates: createSpatialField(c.Address.Location.Latitude, c.Address.Location.Longitude)
           };                            
})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class ProductsSearch : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "ProductsSearch",
                    Maps = new HashSet<string>
                        {
                            @"map('Products', function (p){ 
    return { 
             Name: p.Name  ,
             Category: p.Category,
             Supplier: p.Supplier,
             PricePerUnit: p.PricePerUnit
           };                            
})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }


        private class OrdersTotal : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "OrdersTotal",
                    Maps = new HashSet<string>
                        {
                            @"map('Orders', function (o){ 
function Sum(total, ol)
{
    return total + (ol.Quantity * ol.PricePerUnit) * (1- ol.Discount);
}
    return { 
             Employee: o.Employee  ,
             Company: o.Company,
             Total: o.Lines.reduce(Sum,0)
           };                            
})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class OrdersSearch : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "OrdersSearch",
                    Maps = new HashSet<string>
                        {
                            @"map('Orders', function (o){ 
    return { 
             Address: [o.ShipTo.City, o.ShipTo.Country]  ,
             Products: o.Lines.map(x=>x.Product)
           };                            
})",
                        },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["Address"] = new IndexFieldOptions
                        {
                            TermVector = FieldTermVector.Yes
                        },
                        ["Products"] = new IndexFieldOptions
                        {
                            TermVector = FieldTermVector.Yes
                        }
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }


        private class MultiField : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "MultiField",
                    Maps = new HashSet<string>
                        {
                            @"map('Companies', function (c){ return {Query : [c.ExternalId, c.Name, c.Contact.Name] } })"
                        },
                    Fields = new Dictionary<string, IndexFieldOptions>()
                    {
                        ["Query"] = new IndexFieldOptions
                        {
                            Suggestions = true,
                            Indexing = FieldIndexing.Search
                        }
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class MultiMap : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "MultiMap",
                    Maps = new HashSet<string>
                        {
                            @"map('Employees', function (e){ return {Name: e.FirstName + ' ' + e.LastName}})",
                            @"map('Companies', function (c){ return {Name: c.Contact.Name}})",
                            @"map('Suppliers', function (s){ return {Name: s.Contact.Name}})"
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class EmployeeByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeByName",
                    Maps = new HashSet<string>
                        {
                            @"map('Employees', function (e){ 
                            return { Name: e.FirstName + ' ' + e.LastName };                            
                        })",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class RegionsByTerritoriesName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "RegionsByTerritoriesName",
                    Maps = new HashSet<string>
                        {
                            @"map('Regions', function (r){ 
                            return { Name: r.Territories.map(x=>x.Name) };                            
                        })",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class EmployeeIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeIndex",
                    Maps = new HashSet<string>
                        {
                            @"map('Employees', function (e){ return e})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class EmployeeByHiredAt : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "EmployeeByHiredAt",
                    Maps = new HashSet<string>
                        {
                            @"
                            map('Employees', function (e){var date = new Date(e.HiredAt); return {HiredAt: date, 'HiredAt.Year':date.getYear()}})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class RevenuePerProduct : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "RevenuePerProduct",
                    Maps = new HashSet<string>
                        {
                            @"map('Orders', function (o){
var res = []

o.Lines.forEach(l => {
    res.push({Product: l.Product, Quantity: l.Quantity, Total: (l.Quantity * l.PricePerUnit) * (1 - l.Discount)});
})

return res;
})",
                        },
                    Reduce = @"groupBy( r => r.Product  )
 .aggregate( g => ({
     Product : g.key,
     Quantity : g.values.reduce((sum, x) => x.Quantity + sum, 0),
     Total: g.values.reduce((sum, x) => x.Total + sum, 0)
 }))",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["Product"] = new IndexFieldOptions(),
                        ["Quantity"] = new IndexFieldOptions(),
                        ["Total"] = new IndexFieldOptions()
                    },
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }
    }
}
