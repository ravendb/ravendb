using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17127 : RavenTestBase
    {
        public RavenDB_17127(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_Throw_Offset_Cannot_Be_Negative()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = $"user_{i}",
                            LastName = $"lastname_{i}", 
                            AddressId = $"Address_{i}", 
                            Age = i
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Index.Result, Index>().Where(x => x.Age > 5);

                    var PageSize = 25;
                    var pageNum = 1;
                    var query2 = query
                        .OrderBy(u => u.Name)
                        .ThenBy(u => u.LastName)
                        .ThenBy(u => u.AddressId)
                        .Skip(PageSize * (pageNum - 1))
                        .Take(PageSize);
                    Assert.Equal("from index 'Index' where Age > $p0 order by Name, LastName, AddressId limit $p1, $p2", query2.ToString());
                    var res = query2.ToArray();
                    Assert.Equal(4, res.Length);

                    pageNum = 0;
                    query2 = query
                        .OrderBy(u => u.Name)
                        .ThenBy(u => u.LastName)
                        .ThenBy(u => u.AddressId)
                        .Skip(PageSize * (pageNum - 1))
                        .Take(PageSize);
                    Assert.Equal("from index 'Index' where Age > $p0 order by Name, LastName, AddressId limit $p1, $p2", query2.ToString());

                    var x = Assert.ThrowsAny<RavenException>(() => query2.ToArray());
                    Assert.Contains("Offset (Start) cannot be negative, but was -25.", x.Message);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<User, Index.Result>
        {
            public class Result
            {
                public string Name;
                public string LastName;
                public string AddressId;
                public int Age;
            }

            public Index()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        Name = user.Name,
                        LastName = user.LastName,
                        AddressId = user.AddressId,
                        Age = user.Age
                    };
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string LastName { get; set; }
            public string AddressId { get; set; }
            public int Count { get; set; }
            public int Age { get; set; }
        }
    }
}
