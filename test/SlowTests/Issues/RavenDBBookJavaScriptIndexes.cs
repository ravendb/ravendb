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

        [Fact(Skip="This is just a playground test for javascript indexes")]
        public void PlaygroundTest()
        {
            ExecuteIndexAndWaitForUserToContinueTheTest<SalesByCityAndSupplier>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<CitiesDetails>();            
            //ExecuteIndexAndWaitForUserToContinueTheTest<ProductsDailySale>();            
            //ExecuteIndexAndWaitForUserToContinueTheTest<CompaniesPurchases>();
            //ExecuteIndexAndWaitForUserToContinueTheTest<RevenuePerProductWithAvarage>();            
            //ExecuteIndexAndWaitForUserToContinueTheTest<RevenuePerProduct>();
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
                        $options:{indexing: 'Search', storage: true}
                    })
    }
    return { 
             _: multipleDynamicFields,
             __:  {$value: e.LastName, $name: e.FirstName, $options: {indexing: 'Search', storage: true} } ,
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

        private class RevenuePerProductWithAvarage : AbstractIndexCreationTask
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
    res.push({Product: l.Product, Quantity: l.Quantity, Total: (l.Quantity * l.PricePerUnit) * (1 - l.Discount), Count:1, Average: 0, Debug: [] });
})

return res;
})",
                    },
                    Reduce = @"groupBy( r => r.Product  )
 .aggregate( function (g){
    var total = g.values.reduce((sum, x) => x.Total + sum, 0);
    var count = g.values.reduce((sum, x) => x.Count + sum, 0);
    return {
     Product : g.key,
     Quantity : g.values.reduce((sum, x) => x.Quantity + sum, 0),
     Total: total,
     Count: count,
     Average: total/count,
     Debug: g.values.map( x => x.Total)
 }})",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["Product"] = new IndexFieldOptions(),
                        ["Quantity"] = new IndexFieldOptions(),
                        ["Total"] = new IndexFieldOptions(),
                        ["Count"] = new IndexFieldOptions(),
                        ["Average"] = new IndexFieldOptions(),
                        ["Debug"] = new IndexFieldOptions()
                    },
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class CompaniesPurchases  : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "CompaniesPurchases",
                    Maps = new HashSet<string>
                    {
                        @"map('Orders', function (o){
var res = []

o.Lines.forEach(l => {
    res.push({Company: o.Company, Products: [{Product:l.Product, Quantity:l.Quantity}], Total: l.Quantity });
})

return res;
})",
                    },
                    Reduce = @"groupBy( r => r.Company  )
 .aggregate( function (g){
    var a = {}
    g.values.forEach(x => {
        x.Products.forEach( p => {
        if(a.hasOwnProperty(p.Product) == false)
        {
            a[p.Product] = 0;
        }
        a[p.Product] += p.Quantity
        })
    })
    return {
     Company: g.key,
     Products: Object.keys(a).map(i => ({ Product:i, Quantity: a[i]})),
     Total: g.values.reduce((sum, x) => x.Total + sum, 0),
 }})",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["Company"] = new IndexFieldOptions(),
                        ["Products"] = new IndexFieldOptions(),
                        ["Total"] = new IndexFieldOptions(),
                    },
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class ProductsDailySale : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "ProductsDailySale",
                    Maps = new HashSet<string>
                    {
                        @"map('Orders', function (o){
var res = []
o.Lines.forEach(l => {
    res.push({Date: o.OrderedAt, Product: l.Product, Count : l.Quantity });
})

return res;
})",
                    },
                    Reduce = @"

groupBy( r => ({ Date: r.Date, Product: r.Product }) )
 .aggregate( function (g){


    return {
        Date: g.key.Date,
        Product: g.key.Product,
        Count: g.values.reduce((sum, x) => x.Count + sum, 0)
     }
})",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["Date"] = new IndexFieldOptions(),
                        ["Product"] = new IndexFieldOptions(),
                        ["Count"] = new IndexFieldOptions(),
                    },
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class CitiesDetails : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "CitiesDetails",
                    Maps = new HashSet<string>
                    {
                        @"map('Companies', function (c){
    return {
    City: c.Address.City,
    Companies: 1,
    Suppliers: 0,
    Employees: 0,
    OrderTotal: 0
}
})",
                        @"map('Suppliers', function (s){
    return {
    City: s.Address.City,
    Companies: 0,
    Suppliers: 1,
    Employees: 0,
    OrderTotal: 0
}
})",
                        @"map('Employees', function (e){
    return {
    City: e.Address.City,
    Companies: 0,
    Suppliers: 0,
    Employees: 1,
    OrderTotal: 0
}
})",
                        @"map('Orders', function (o){
    function Sum(total, ol)
    {
        return total + (ol.Quantity * ol.PricePerUnit) * (1- ol.Discount);
    }
    return {
    City: o.ShipTo.City,
    Companies: 0,
    Suppliers: 0,
    Employees: 0,
    OrderTotal: o.Lines.reduce(Sum,0)
}
})"
                    },
                    Reduce = @"

groupBy( r => r.City )
 .aggregate( function (g){


    return {
        City : g.key,
        Companies:  g.values.reduce((sum, x) => x.Companies + sum, 0), 
        Suppliers:  g.values.reduce((sum, x) => x.Suppliers + sum, 0), 
        Employees:  g.values.reduce((sum, x) => x.Employees + sum, 0),
        OrderTotal: g.values.reduce((sum, x) => x.OrderTotal + sum, 0)
     }
})",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["Companies"] = new IndexFieldOptions(),
                        ["Suppliers"] = new IndexFieldOptions(),
                        ["Employees"] = new IndexFieldOptions(),
                        ["OrderTotal"] = new IndexFieldOptions(),
                    },
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class SalesByCityAndSupplier : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "SalesByCityAndSupplier",
                    Maps = new HashSet<string>
                    {
                        @"map('Orders', function (o){
var res = []
function getMonth(s){
    var d = new Date(s);
    return d.getMonth()
}

function getYear(s){
    var d = new Date(s);
    return d.getFullYear()
}
o.Lines.forEach(l => {
    res.push({City: o.ShipTo.City, Month: getMonth(o.ShippedAt), Year: getYear(o.ShippedAt), Supplier: load(l.Product,'Products').Supplier, Total: l.PricePerUnit * l.Quantity
 });
})

return res;
})"
                    },
                    Reduce = @"
groupBy( r =>  ({City: r.City, Supplier: r.Supplier, Month: r.Month, Year: r.Year }) )
 .aggregate( function (g){
    return {
        City : g.key.City,
        Supplier:  g.key.Supplier,
        Month:  g.key.Month, 
        Year:  g.key.Year,
        Total: g.values.reduce((sum, x) => x.Total + sum, 0)
     }
})",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        ["City"] = new IndexFieldOptions(),
                        ["Month"] = new IndexFieldOptions(),
                        ["Year"] = new IndexFieldOptions(),
                        ["Supplier"] = new IndexFieldOptions(),
                        ["Total"] = new IndexFieldOptions(),                        
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
