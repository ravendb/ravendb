using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class QueryTests : RavenTestBase
    {
        public QueryTests(ITestOutputHelper output) : base(output)
        {
        }

        private class A
        {
            public B B { get; set; }
        }

        private class B
        {
            public uint Uint { get; set; }
            public long Long { get; set; }
            public ulong Ulong { get; set; }
            public short Short { get; set; }
            public ushort Ushort { get; set; }
            public char Char { get; set; }
            public sbyte Sbyte { get; set; }
            public byte Byte { get; set; }
        }

        private class Article
        {
            public string Title { get; set; }
            public string Description { get; set; }
            public bool IsDeleted { get; set; }
        }
        
        [Fact]
        public  void Query_CreateClausesForQueryDynamicallyWithOnBeforeQueryEvent()
        {
            using (var store = GetDocumentStore())
            {
                const string id1 = "users/1";
                const string id2 = "users/2";
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = false
                            
                        },
                        id1);
                    
                    session.Store(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = true
                        },
                        id2);
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Advanced.OnBeforeQuery += (sender, args) =>
                    {
                        var queryToBeExecuted = (AsyncDocumentQuery<Article>) args.QueryCustomization;
                        queryToBeExecuted.AndAlso(wrapPreviousQueryClauses: true);
                        queryToBeExecuted.WhereEquals(nameof(Article.IsDeleted), true);

                    };

                    var query = session.Advanced.DocumentQuery<Article>()
                        .Search(article => article.Title, "foo")
                        .Search(article => article.Description, "bar", @operator: SearchOperator.Or);
                    
                    Assert.Equal(query.ToString(), "from 'Articles' where (search(Title, $p0) or search(Description, $p1)) and IsDeleted = $p2");
                    
                    var result =  query.ToList();
                    Assert.Equal(result.Count, 1);
                }
            }
        }
        [Fact]
        public async Task Query_CreateClausesForQueryDynamicallyWhenTheQueryEmpty()
        {
            using (var store = GetDocumentStore())
            {
                const string id1 = "users/1";
                const string id2 = "users/2";
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = false
                            
                        },
                        id1);
                    
                    session.Store(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = true
                        },
                        id2);
                    session.SaveChanges();
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<Article>()
                        .AndAlso(wrapPreviousQueryClauses: true);
                    
                    Assert.Equal(query.ToString(),"from 'Articles'");
                    var queryResult =  await query.ToListAsync();
                    Assert.Equal(queryResult.Count, 2);

                }
            }
        }
        
        [Fact]
        public  void Query_CreateClausesForQueryDynamically()
        {
            using (var store = GetDocumentStore())
            {
                const string id1 = "users/1";
                const string id2 = "users/2";
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = false
                            
                        },
                        id1);
                    
                    session.Store(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = true
                        },
                        id2);
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {

                    var query = session.Advanced.DocumentQuery<Article>()
                        .Search(article => article.Title, "foo")
                        .Search(article => article.Description, "bar", @operator: SearchOperator.Or)
                        .AndAlso(wrapPreviousQueryClauses: true).WhereEquals(x => x.IsDeleted, false);
                    
                    Assert.Equal(query.ToString(), "from 'Articles' where (search(Title, $p0) or search(Description, $p1)) and IsDeleted = $p2");
                    
                    var result =  query.ToList();
                    Assert.Equal(result.Count, 1);
                }
            }
        }
        
        [Fact]
        public async Task Query_CreateClausesForQueryDynamicallyAsyncWithOnBeforeQueryEvent()
        {
            using (var store = GetDocumentStore())
            {
                const string id1 = "users/1";
                const string id2 = "users/2";
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = false
                            
                        },
                        id1);
                    
                   await session.StoreAsync(new Article
                        {
                            Title = "foo",
                            Description = "bar",
                            IsDeleted = true
                        },
                        id2);
                   
                   await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.OnBeforeQuery += (sender, args) =>
                    {
                        var queryToBeExecuted = (AsyncDocumentQuery<Article>) args.QueryCustomization;
                        queryToBeExecuted.AndAlso(wrapPreviousQueryClauses: true);
                        queryToBeExecuted.WhereEquals(nameof(Article.IsDeleted), true);

                    };

                    var query = session.Advanced.AsyncDocumentQuery<Article>()
                        .Search(article => article.Title, "foo")
                        .Search(article => article.Description, "bar", @operator: SearchOperator.Or);
                    
                    var result = await query.ToListAsync();
                    Assert.Equal(query.ToString(), "from 'Articles' where (search(Title, $p0) or search(Description, $p1)) and IsDeleted = $p2");
                    Assert.Equal(result?.Count,1);
                }
            }
        }
        
        [Fact]
        public async Task Query_WhenCompareObjectWithUlongInWhereClause_ShouldWork()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await StoreAsync(session, 2);
                await StoreAsync(session, 1);
                await StoreAsync(session, 1);
                await StoreAsync(session, 0);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                _ = await session.Query<A>().Where(x => x.B == new B { Uint = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Long = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Ulong = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Short = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Ushort = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Char = (char)1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Byte = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Sbyte = 1 }).ToArrayAsync();
            }
        }

        [Fact]
        public async Task Query_WhenUsingDateTimeNowInWhereClause_ShouldSendRequestForEachQuery()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                const int numberOfRequests = 2;
                for (var i = 0; i < numberOfRequests; i++)
                {
                    _ = await session.Query<Order>()
                        .Where(x => x.OrderedAt < DateTime.Now)
                        .Take(5)
                        .ToListAsync();
                }

                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }

        private static async Task StoreAsync(IAsyncDocumentSession session, int value)
        {
            await session.StoreAsync(new A
            {
                B = new B { Uint = (uint)value, Long = value, Ulong = (ulong)value, Short = (short)value, Ushort = (ushort)value, Char = (char)value, Byte = (byte)value, Sbyte = (sbyte)value }
            });
        }
    }
}
