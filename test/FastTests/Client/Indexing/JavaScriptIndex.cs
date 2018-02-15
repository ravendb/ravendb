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
                    //WaitForUserToContinueTheTest(store);
                    session.Query<User>("UsersByName").Single(x => x.Name == "Brendan Eich");
                }
                
            }
        }

        private class User
        {
            public string Name { get; set; }
            public bool IsActive { get; set; }
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
    }
}
