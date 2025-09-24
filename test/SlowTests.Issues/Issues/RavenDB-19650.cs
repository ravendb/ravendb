using System.Collections.Generic;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Indexes;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_19650 : RavenTestBase
    { 
        public RavenDB_19650(ITestOutputHelper output) : base(output)
        {
          
        }
        
        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanProjectDataFromBothIndexAndDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new Content_ByUrl());

                using (var session = store.OpenSession())
                {
                    session.Store(new Post { Id = "posts/1" });
                    session.Store(new Post { Id = "posts/2", Slug = "posts-2", Origin = new OriginReference("posts/1", "Posts") });
                    session.Store(new Post { Id = "posts/3", DisplayName = "Link name", Slug = "posts-3", Origin = new OriginReference("posts/2", "Posts") });
                    session.Store(new Post { Id = "posts/4", Slug = "posts-4", Origin = new OriginReference("posts/3", "Posts") });
                    session.Store(new Post { Id = "posts/5", Slug = "posts-5", Origin = new OriginReference("posts/4", "Posts") });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    WaitForUserToContinueTheTest(store);

                    var results = (session.Query<Content_ByUrl.Result, Content_ByUrl>().ProjectInto<Content_ByUrl.ProjectionResult>()).Single(x => x.Url == "/posts-2/posts-3/");
          
                    Assert.Equal("posts/3", results.Ref);
                    Assert.Equal("Link name", results.DisplayName);
                }
            }
        }
        
        private record Post
        {
            public string Id { get; set; }
            public string DisplayName { get; set; }
            public string Slug { get; set; } = string.Empty;
            public OriginReference Origin { get; set; }
        }
        
        private record OriginReference(string Id, string Collection);
        
        private class Content_ByUrl : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Ref { get; set; }
                public string Collection { get; set; }
                public string Parent { get; set; }
                public string Url { get; set; }
            }

            public class ProjectionResult
            {
                public string Ref { get; set; }
                public string Collection { get; set; }
                public string Parent { get; set; }
                public string Url { get; set; }
                public string DisplayName { get; set; }
            }

            public Content_ByUrl()
            {
                Maps = new HashSet<string>
                { 
                    @"map('Pages', p => {
              let ref = id(p)
              let url = ''
              let collection = p['@metadata']['@collection']
              let parent = p.Origin.Id
              let visited = {}
              do {
                url = p.Slug + '/' + url
                if (visited[p.Origin.Id])
                  break
                visited[p.Origin.Id] = true
                p = load(p.Origin.Id, p.Origin.Collection)
              } while(p);
              return { Ref: ref, Collection: collection, Parent: parent, Url: url };
          });",
                    @"map('Homes', p => {
              let ref = id(p)
              let collection = p['@metadata']['@collection']
              let parent = p.Origin.Id
              let url = ''
              let visited = {}
              do {
                url = p.Slug + '/' + url
                if (visited[p.Origin.Id])
                  break
                visited[p.Origin.Id] = true
                p = load(p.Origin.Id, p.Origin.Collection)
              } while(p);
              return { Ref: ref, Collection: collection, Parent: parent, Url: url };
          });",
                    @"map('Posts', p => {
              let ref = id(p)
              let collection = p['@metadata']['@collection']
              let parent = p.Origin.Id
              let url = ''
              let visited = {}
              do {
                url = p.Slug + '/' + url
                if (visited[p.Origin.Id])
                  break
                visited[p.Origin.Id] = true
                p = load(p.Origin.Id, p.Origin.Collection)
              } while(p);
              return { Ref: ref, Collection: collection, Parent: parent, Url: url };
          });"
                };

                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "Ref", new IndexFieldOptions()
                        {
                            Storage = FieldStorage.Yes,
                        }
                    },
                    {
                        "Url", new IndexFieldOptions()
                        {
                            Storage = FieldStorage.Yes,
                        }
                    }
                };
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadDocumentFromReferenceExistingOnlyInIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var m1 = new Manager() { Name = "CoolName", Age = 21};
                    session.Store(m1, "managers/1");

                    var e1 = new Employee() { ManId = m1.Id, Salary = 37};
                    session.Store(e1, "employees/1$managers/1");

                    var o1 = new Order() { EmpId = e1.Id, Price = 44, Name = "OrderName"};
                    session.Store(o1, "orders/1$managers/1");
                    
                    session.SaveChanges();
                    
                    var index = new DummyIndex();
                    store.ExecuteIndex(index);
                    Indexes.WaitForIndexing(store);
                    
                    WaitForUserToContinueTheTest(store);

                    var query = from res in session.Query<DummyIndex.Result>(index.IndexName)
                        let manager = RavenQuery.Load<Manager>(res.ManRef)
                        select manager.Name;

                    var result = query.ToList();

                    Assert.Equal("CoolName", result[0]);
                }
            }
        }

        private class Manager
        {
            public string Id { get; set; }
            public string Name { get; set; }
            
            public int Age { get; set; }
        }

        private class Employee
        {
            public string Id { get; set; }
            public string ManId { get; set; }
            
            public int Salary { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string EmpId { get; set; }
            
            public int Price { get; set; }
            
            public string Name { get; set; }
        }

        private class DummyIndex : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string ManRef { get; set; }
            }
            public DummyIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('Orders', order => {
                        let emp = load(order.EmpId, ""Employees"");
                        return { ManRef: emp.ManId};
                    });"
                };
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "ManRef", new IndexFieldOptions()
                        {
                            Storage = FieldStorage.Yes,
                        }
                    }
                };
            }
        }
    }
}
