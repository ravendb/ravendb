using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class JavaScriptIndex:RavenTestBase
    {

        [Fact]
        public void CanUseJavaScriptIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User{Name = "Brendan Eich" , IsActive = true});
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersByName").Single(x => x.Name == "Brendan Eich");
                }
                
            }
        }

        [Fact]
        public void CanUseJavaScriptMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersAndProductsByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Brendan Eich", IsActive = true });
                    session.Store(new Product {Name = "Shampoo", IsAvailable = true});
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersAndProductsByName").Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        [Fact]
        public void CanUseJavaScriptMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersAndProductsByNameAndCount());
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Brendan Eich", IsActive = true });
                    session.Store(new Product { Name = "Shampoo", IsAvailable = true });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersAndProductsByNameAndCount").OfType<ReduceResults>().Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        private class User
        {
            public string Name { get; set; }
            public bool IsActive { get; set; }
        }

        private class Product
        {
            public string Name { get; set; }
            public bool IsAvailable { get; set; }
        }

        private class ReduceResults
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }
        private class UsersByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersByName",
                    Maps = new HashSet<string>
                    {
                        "collection(\'Users\')\r\n    .map(function (u) { \r\n        return { Name: u.Name, Count: 1}; \r\n    });"
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,                    
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class UsersAndProductsByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersAndProductsByName",
                    Maps = new HashSet<string>
                    {
                        "collection(\'Users\')\r\n    .map(function (u) { \r\n        return { Name: u.Name, Count: 1}; \r\n    });",
                        "collection(\'Products\')\r\n    .map(function (p) { \r\n        return { Name: p.Name, Count: 1}; \r\n    });"
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class UsersAndProductsByNameAndCount : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersAndProductsByNameAndCount",
                    Maps = new HashSet<string>
                    {
                        "collection(\'Users\')\r\n    .map(function (u) { \r\n        return { Name: u.Name, Count: 1}; \r\n    });",
                        "collection(\'Products\')\r\n    .map(function (p) { \r\n        return { Name: p.Name, Count: 1}; \r\n    });"
                    },
                    Reduce = "groupBy(x => x.Name)\r\n    .aggregate((key,values) => {\r\n        return {\r\n            Name: key,\r\n            Count: values.reduce((total, val) => val.Count + total,0)\r\n        };\r\n    });\r\n",
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }
    }
}
