using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_16556 : RavenTestBase
{
    public RavenDB_16556(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(@"from Orders as o
                                        group by o.CompId, o.EmpId
                                        order by count() as long desc
                                        load o.EmpId as e
                                        select 
                                        {
                                            Key: key(),
                                            Value: sum(x => x.Price)
                                        }")]
    [InlineData(@"declare function 
                    output(o){
                        return {
                            Key: key(),
                            Value: sum(x => x.Price)
                        }
                    }

                from Orders as o
                group by o.CompId, o.EmpId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    [InlineData(@"declare function 
                    output(o){
                        return {
                            Key: key(),
                            Value: sum(x => x.Price)
                        }
                    }

                from Orders as o
                group by o.CompId, o.EmpId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    public void CanGetKeyWithMultipleGroupByFieldsFromAutoMapReduceIndexViaJs(string query)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            Employee e1 = new() { Name = "Maciej" };
            Employee e2 = new() { Name = "Michal" };

            Company c1 = new() { Name = "Company1" };
            Company c2 = new() { Name = "Company2" };

            session.Store(e1);
            session.Store(e2);

            session.Store(c1);
            session.Store(c2);
            
            Order o1 = new (){EmpId = e1.Id, CompId = c1.Id, Price = 21};
            Order o2 = new (){EmpId = e2.Id, CompId = c2.Id, Price = 37};
            Order o3 = new (){EmpId = e1.Id, CompId = c2.Id, Price = 44};
            
            session.Store(o1);
            session.Store(o2);
            session.Store(o3);

            session.SaveChanges();
            
            var res = session.Advanced
                .RawQuery<DtoWithJson>(query)
                .WaitForNonStaleResults().ToList();

            var dict1 = new Dictionary<string, object> { { "CompId", "companies/1-A" }, { "EmpId", "employees/1-A" } };
            var dict2 = new Dictionary<string, object> { { "CompId", "companies/2-A" }, { "EmpId", "employees/2-A" } };
            var dict3 = new Dictionary<string, object> { { "CompId", "companies/2-A" }, { "EmpId", "employees/1-A" } };

            var expected = new List<DtoWithJson>{
                new(){Value = 21, Key = dict1 }, 
                new(){Value = 37, Key = dict2 },
                new(){Value = 44, Key = dict3 }
            };

            Assert.Equal(expected[0].Value, res[0].Value);
            Assert.Equal(expected[1].Value, res[1].Value);
            Assert.Equal(expected[2].Value, res[2].Value);
            
            Assert.Equal(expected[0].Key, res[0].Key);
            Assert.Equal(expected[1].Key, res[1].Key);
            Assert.Equal(expected[2].Key, res[2].Key);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(@"from Orders as o
                                        group by o.CompId
                                        order by count() as long desc
                                        load o.EmpId as e
                                        select 
                                        {
                                            Key: key(),
                                            Value: count()
                                        }")]
    [InlineData(@"declare function 
                    output(o){
                        return {
                            Key: key(),
                            Value: count()
                        }
                    }

                from Orders as o
                group by o.CompId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    public void CanGetKeyAndCountFromAutoMapReduceIndexViaJs(string query)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            Employee e1 = new() { Name = "Maciej" };
            Employee e2 = new() { Name = "Michal" };

            Company c1 = new() { Name = "Company1" };
            Company c2 = new() { Name = "Company2" };

            session.Store(e1);
            session.Store(e2);

            session.Store(c1);
            session.Store(c2);
            
            Order o1 = new (){EmpId = e1.Id, CompId = c1.Id, Price = 21};
            Order o2 = new (){EmpId = e2.Id, CompId = c2.Id, Price = 37};
            Order o3 = new (){EmpId = e1.Id, CompId = c2.Id, Price = 44};
            
            session.Store(o1);
            session.Store(o2);
            session.Store(o3);

            session.SaveChanges();

            var res = session.Advanced
                .RawQuery<Dto>(query)
                .WaitForNonStaleResults().ToList();
            
            var expected = new List<Dto>{new(){Value = 2, Key = "companies/2-A"}, new(){Value = 1, Key = "companies/1-A"}};
            
            Assert.Equal(expected[0].Key, res[0].Key);
            Assert.Equal(expected[1].Key, res[1].Key);
            
            Assert.Equal(expected[0].Value, res[0].Value);
            Assert.Equal(expected[1].Value, res[1].Value);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(@"from Orders as o
                                        group by o.CompId
                                        order by count() as long desc
                                        load o.EmpId as e
                                        select 
                                        {
                                            Key: key(),
                                            Value: sum()
                                        }")]
    [InlineData(@"declare function 
                    output(o){
                        return {
                            Key: key(),
                            Value: sum()
                        }
                    }

                from Orders as o
                group by o.CompId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    [InlineData(@"declare function 
                    output(o){
                    var empId = key().EmpId;
                        return {
                            Key: empId,
                            Value: sum()
                        }
                    }

                from Orders as o
                group by o.CompId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    public void CheckIfInvalidOperationExceptionIsThrownForIncorrectNumberOfArgsInSum(string query)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            Employee e1 = new() { Name = "Maciej" };
            Employee e2 = new() { Name = "Michal" };

            Company c1 = new() { Name = "Company1" };
            Company c2 = new() { Name = "Company2" };

            session.Store(e1);
            session.Store(e2);

            session.Store(c1);
            session.Store(c2);

            Order o1 = new() { EmpId = e1.Id, CompId = c1.Id, Price = 21 };
            Order o2 = new() { EmpId = e2.Id, CompId = c2.Id, Price = 37 };
            Order o3 = new() { EmpId = e1.Id, CompId = c2.Id, Price = 44 };

            session.Store(o1);
            session.Store(o2);
            session.Store(o3);

            session.SaveChanges();
            
            RavenException ex = Assert.Throws<RavenException>(() =>
            {
                var res = session.Advanced
                    .RawQuery<Dto>(query)
                    .WaitForNonStaleResults().ToList();
            });
            
            Assert.Contains("sum(doc => doc.fieldName) must be called with a single arrow function expression argument", ex.Message);
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(@"from Orders as o
                                        group by o.CompId
                                        order by count() as long desc
                                        load o.EmpId as e
                                        select 
                                        {
                                            Key: key(),
                                            Value: count(2)
                                        }")]
    [InlineData(@"declare function 
                    output(o){
                        return {
                            Key: key(),
                            Value: count(x => x.Freight)
                        }
                    }

                from Orders as o
                group by o.CompId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    [InlineData(@"declare function 
                    output(o){
                    var empId = key().EmpId;
                        return {
                            Key: empId,
                            Value: count(""Freight"")
                        }
                    }

                from Orders as o
                group by o.CompId
                order by count() as long desc
                load o.EmpId as e
                select output(o)")]
    public void CheckIfInvalidOperationExceptionIsThrownForIncorrectNumberOfArgsInCount(string query)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            Employee e1 = new() { Name = "Maciej" };
            Employee e2 = new() { Name = "Michal" };

            Company c1 = new() { Name = "Company1" };
            Company c2 = new() { Name = "Company2" };

            session.Store(e1);
            session.Store(e2);

            session.Store(c1);
            session.Store(c2);

            Order o1 = new() { EmpId = e1.Id, CompId = c1.Id, Price = 21 };
            Order o2 = new() { EmpId = e2.Id, CompId = c2.Id, Price = 37 };
            Order o3 = new() { EmpId = e1.Id, CompId = c2.Id, Price = 44 };

            session.Store(o1);
            session.Store(o2);
            session.Store(o3);

            session.SaveChanges();
            
            RavenException ex = Assert.Throws<RavenException>(() =>
            {
                var res = session.Advanced
                    .RawQuery<Dto>(query)
                    .WaitForNonStaleResults().ToList();
            });
            
            Assert.Contains("count() must be called without arguments", ex.Message);
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(@"from Orders as o
                                        group by o.CompId
                                        order by count() as long desc
                                        load o.EmpId as e
                                        select 
                                        {
                                            Value: sum(x)
                                        }")]
    public void CheckIfInvalidOperationExceptionIsThrownForIncorrectArgumentInSum(string query)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            Employee e1 = new() { Name = "Maciej" };
            Employee e2 = new() { Name = "Michal" };

            Company c1 = new() { Name = "Company1" };
            Company c2 = new() { Name = "Company2" };

            session.Store(e1);
            session.Store(e2);

            session.Store(c1);
            session.Store(c2);

            Order o1 = new() { EmpId = e1.Id, CompId = c1.Id, Price = 21 };
            Order o2 = new() { EmpId = e2.Id, CompId = c2.Id, Price = 37 };
            Order o3 = new() { EmpId = e1.Id, CompId = c2.Id, Price = 44 };

            session.Store(o1);
            session.Store(o2);
            session.Store(o3);

            session.SaveChanges();
            
            RavenException ex = Assert.Throws<RavenException>(() =>
            {
                var res = session.Advanced
                    .RawQuery<Dto>(query)
                    .WaitForNonStaleResults().ToList();
            });
            
            Assert.Contains("sum(doc => doc.fieldName) must be called with arrow function expression that points to field you want to aggregate", ex.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.Querying)]
    public void CheckIfInvalidOperationExceptionIsThrownForUsingKeyInStaticIndexQuery()
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            Employee e1 = new() { Name = "Maciej" };
            Employee e2 = new() { Name = "Michal" };

            Company c1 = new() { Name = "Company1" };
            Company c2 = new() { Name = "Company2" };

            session.Store(e1);
            session.Store(e2);

            session.Store(c1);
            session.Store(c2);

            Order o1 = new() { EmpId = e1.Id, CompId = c1.Id, Price = 21 };
            Order o2 = new() { EmpId = e2.Id, CompId = c2.Id, Price = 37 };
            Order o3 = new() { EmpId = e1.Id, CompId = c2.Id, Price = 44 };

            session.Store(o1);
            session.Store(o2);
            session.Store(o3);

            var index = new OrdersByCompany() ;
            var query = $@"from index ""{index.IndexName}"" as o
                                        select 
                                        {{
                                            Key: key()
                                        }}";
            
            index.Execute(store);
            
            Indexes.WaitForIndexing(store);

            session.SaveChanges();
            
            RavenException ex = Assert.Throws<RavenException>(() =>
            {
                var res = session.Advanced
                    .RawQuery<Dto>(query)
                    .WaitForNonStaleResults().ToList();
            });
            
            Assert.Contains("key() can only be used with dynamic index", ex.Message);
        }
    }

    private class OrdersByCompany : AbstractIndexCreationTask<Order, OrdersByCompany.ReduceMap>
    {
        public class ReduceMap
        {
            public string Name { get; set; }
            public int Sum { get; set; }
        }
        public OrdersByCompany()
        {
            Map = orders => from order in orders
                select new ReduceMap() { Name = order.CompId, Sum = 1 };
            
            Reduce = results => from result in results
                group result by result.Name
                into g
                select new ReduceMap() { Name = g.Key, Sum = g.Sum(x => x.Sum) };
        }
    }

    private class Order
    {
        public string Id { get; set; }
        public string EmpId { get; set; }
        public string CompId { get; set; }
        
        public int Price { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }

        public string Name { get; set; }
    }

    private class Employee
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Dto
    {
        public string Key { get; set; }
        public int Value { get; set; }
    }
    
    private class DtoWithJson
    {
        public Dictionary<string, object> Key { get; set; }
        public int Value { get; set; }
    }
}
