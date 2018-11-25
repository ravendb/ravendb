using System;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12346 : RavenTestBase
    {
        [Fact]
        public void CanConvertFromQueryToDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var docQuery = session.Advanced.DocumentQuery<User>();
                    var q = docQuery.ToQueryable();
                    q = q.Where(x => x.Name == "123");
                    q = q.Search(x => x.Age, "123");

                    docQuery = q.ToDocumentQuery();

                    Assert.Equal(q.ToString(), docQuery.ToString());
                    Assert.Equal("from Users where Name = $p0 and search(Age, $p1) select id() as Id, Name, LastName, AddressId, Count, Age", docQuery.ToString());

                    var e = Assert.Throws<InvalidOperationException>(() => q.ToAsyncDocumentQuery());
                    Assert.Equal("Cannot convert sync query to async document query.", e.Message);

                    q = docQuery
                        .ToQueryable()
                        .Where(x => x.Age > 20);

                    docQuery = q.ToDocumentQuery();

                    Assert.Equal(q.ToString(), docQuery.ToString());
                    Assert.Equal("from Users where Name = $p0 and search(Age, $p1) or Age > $p2 select id() as Id, Name, LastName, AddressId, Count, Age", docQuery.ToString());
                }

                using (var asyncSession = store.OpenAsyncSession())
                {
                    var docQuery = asyncSession.Advanced.AsyncDocumentQuery<User>();
                    var q = docQuery.ToQueryable();
                    q = q.Where(x => x.Name == "123");
                    q = q.Search(x => x.Age, "123");

                    docQuery = q.ToAsyncDocumentQuery();

                    Assert.Equal(q.ToString(), docQuery.ToString());
                    Assert.Equal("from Users where Name = $p0 and search(Age, $p1) select id() as Id, Name, LastName, AddressId, Count, Age", docQuery.ToString());

                    var e = Assert.Throws<InvalidOperationException>(() => q.ToDocumentQuery());
                    Assert.Equal("Cannot convert async query to sync document query.", e.Message);

                    q = docQuery
                        .ToQueryable()
                        .Where(x => x.Age > 20);

                    docQuery = q.ToAsyncDocumentQuery();

                    Assert.Equal(q.ToString(), docQuery.ToString());
                    Assert.Equal("from Users where Name = $p0 and search(Age, $p1) or Age > $p2 select id() as Id, Name, LastName, AddressId, Count, Age", docQuery.ToString());
                }
            }
        }
    }
}
