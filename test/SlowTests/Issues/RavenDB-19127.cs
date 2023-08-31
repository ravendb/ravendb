using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19127: RavenTestBase
{
    public RavenDB_19127(ITestOutputHelper output) : base(output)
    {

    }

    [RavenFact(RavenTestCategory.Querying)]
    public void Streaming_Query_On_Index_With_Load()
    {
        using (var store = GetDocumentStore())
        {
            var definition = new IndexDefinitionBuilder<User>("UsersByNameAndFriendId")
            {
                Map = docs => from doc in docs
                    select new { doc.Name, doc.FriendId }
            }.ToIndexDefinition(store.Conventions);
            store.Maintenance.Send(new PutIndexesOperation(definition));

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Jerry", LastName = "Garcia", FriendId = "users/2" }, "users/1");
                session.Store(new User { Name = "Bob", LastName = "Weir", FriendId = "users/1" }, "users/2");
                session.Store(new User { Name = "Pigpen", FriendId = "users/1" }, "users/3");
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var query = from u in session.Query<User>("UsersByNameAndFriendId")
                    where u.Name != "Pigpen"
                    let friend = RavenQuery.Load<User>(u.FriendId)
                    select new { Name = u.Name, Friend = friend.Name };

                Assert.Equal("from index \'UsersByNameAndFriendId\' as u where u.Name != $p0 " +
                             "load u.FriendId as friend select { Name : u.Name, Friend : friend.Name }"
                    , query.ToString());

                var queryResult = session.Advanced.Stream(query);

                List<dynamic> resList = new();
                while (queryResult.MoveNext())
                {
                    var cur = queryResult.Current.Document;
                    resList.Add(cur);
                }

                Assert.Equal(2, resList.Count);

                Assert.Equal("Jerry", resList[0].Name);
                Assert.Equal("Bob", resList[0].Friend);

                Assert.Equal("Bob", resList[1].Name);
                Assert.Equal("Jerry", resList[1].Friend);
            }
        }
    }
    
    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string LastName { get; set; }
        public DateTime Birthday { get; set; }
        public int IdNumber { get; set; }
        public bool IsActive { get; set; }
        public string[] Roles { get; set; }
        public string DetailId { get; set; }
        public string FriendId { get; set; }
        public IEnumerable<string> DetailIds { get; set; }
        public List<Detail> Details { get; set; }
        public string DetailShortId { get; set; }
        public List<string> Groups { get; set; }
    }
    
    [Fact]
    public void CanLoadDocumentThatWasAlreadyHandledByStreamingQuery()
    {
        using var store = GetDocumentStore();

        using(var session = store.OpenSession())
        {
            session.Store(new Employee("Jane", null), "emps/jane");
            session.Store(new Employee("Sandra", "emps/jane"));
            session.SaveChanges();
        }
        
        using (var session = store.OpenSession())
        {
            var query = from e in session.Query<Employee>()
                let r = RavenQuery.Load<Employee>(e.Manager)
                select new { e = e.FirstName, r = r.FirstName };    
            
            
            var queryResult = session.Advanced.Stream(query);
                
            List<dynamic> resList = new();
            while (queryResult.MoveNext())
            {
                var cur = queryResult.Current.Document;
                resList.Add(cur);
            }
        }
    }
    
    private class Employee
    {
        public Employee(string FirstName, string Manager)
        {
            this.FirstName = FirstName;
            this.Manager = Manager;
        }

        public string FirstName { get; }
        public string Manager { get; }

    }

    private class Detail
    {
        public Detail()
        {

        }
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
