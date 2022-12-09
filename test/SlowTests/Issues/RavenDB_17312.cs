using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17312 : RavenTestBase
{
    public RavenDB_17312(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
    public void JintPropertyAccessorMustGuaranteeTheOrderOfProperties()
    {
        using (var store = GetDocumentStore())
        {
            store.ExecuteIndex(new UsersReducedByNameAndLastName());

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 33 });
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 34});
                
                session.SaveChanges();
                
                Indexes.WaitForIndexing(store);
                
                var results = session.Query<User>("UsersReducedByNameAndLastName").OfType<ReduceResults>().ToList();
                
                Assert.Equal(1, results.Count);

                Assert.Equal(2, results[0].Count);
                Assert.Equal("Joe", results[0].Name);
                Assert.Equal("Doe", results[0].LastName);
            }
        }
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
    public void JintPropertyAccessorMustGuaranteeTheOrderOfPropertiesMultiMapIndex()
    {
        using (var store = GetDocumentStore())
        {
            store.ExecuteIndex(new UsersAndEmployeesReducedByNameAndLastName());

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 33 });
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 34 });

                session.Store(new Employee { FirstName = "Joe", LastName = "Doe", ReportsTo = null });
                session.Store(new Employee { FirstName = "Joe", LastName = "Doe", ReportsTo = "employees/1-A" });

                session.SaveChanges();

                Indexes.WaitForIndexing(store);

                var results = session.Query<User>("UsersAndEmployeesReducedByNameAndLastName").OfType<ReduceResults>().ToList();

                Assert.Equal(1, results.Count);

                Assert.Equal(4, results[0].Count);
                Assert.Equal("Joe", results[0].Name);
                Assert.Equal("Doe", results[0].LastName);
            }
        }
    }

    private class ReduceResults
    {
        public string Name { get; set; }

        public string LastName { get; set; }

        public int Count { get; set; }
    }

    private class UsersReducedByNameAndLastName : AbstractJavaScriptIndexCreationTask
    {
        public UsersReducedByNameAndLastName()
        {
            Maps = new HashSet<string>
            {
                // we're forcing here different order of fields of returned results based on Age property

                @"map('Users', function (u){ 
                    
                    if (u.Age % 2 == 0)
                    {
                        return { Count: 1, Name: u.Name, LastName: u.LastName };
                    }

                    return {  LastName: u.LastName, Name: u.Name, Count: 1};
                })",

            };
            Reduce = @"groupBy(x => { return { Name: x.Name, LastName: x.LastName } })
                                .aggregate(g => {return {
                                    Name: g.key.Name,
                                    LastName: g.key.LastName,
                                    Count: g.values.reduce((total, val) => val.Count + total,0)
                               };})";

        }
    }

    private class UsersAndEmployeesReducedByNameAndLastName : AbstractJavaScriptIndexCreationTask
    {
        public UsersAndEmployeesReducedByNameAndLastName()
        {
            Maps = new HashSet<string>
            {
                // we're forcing here different order of fields of returned results based on Age property

                @"map('Users', function (u){ 
                    
                    if (u.Age % 2 == 0)
                    {
                        return { Count: 1, Name: u.Name, LastName: u.LastName };
                    }

                    return {  LastName: u.LastName, Name: u.Name, Count: 1};
                })",

                // we're forcing here different order of fields of returned results based on ReportsTo property

                @"map('Employees', function (e){ 
                    
                    if (e.ReportsTo == null)
                    {
                        return { Count: 1, Name: e.FirstName, LastName: e.LastName };
                    }

                    return {  LastName: e.LastName, Name: e.FirstName, Count: 1};
                })",
            };
            Reduce = @"groupBy(x => { return { Name: x.Name, LastName: x.LastName } })
                                .aggregate(g => {return {
                                    Name: g.key.Name,
                                    LastName: g.key.LastName,
                                    Count: g.values.reduce((total, val) => val.Count + total,0)
                               };})";

        }
    }
}
