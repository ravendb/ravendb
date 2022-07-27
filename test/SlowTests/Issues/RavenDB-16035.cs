using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16035 : RavenTestBase
    {
        public RavenDB_16035(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
        }
        private class Item{}

        [Fact]
        public void CanMixLazyAndAggressiveCaching()
        {
            bool clearCache = false;
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.OnSucceedRequest += (sender, args) =>
                    {
                        if (clearCache)
                            documentStore.GetRequestExecutor().Cache.Clear();
                    };

                }
            });
            
            using (var s = store.OpenSession())
            {
                s.Store(new User {Name = "Arava"});
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var l1 = s.Query<User>().Where(x => x.Name == "Arava").Lazily();
                var l2 = s.Query<User>().Where(x => x.Name == "Phoebe").Lazily();
                var l3 = s.Query<User>().Where(x => x.Name != null).CountLazily();

                Assert.NotEmpty(l1.Value);
                Assert.Empty(l2.Value);
                Assert.Equal(1, l3.Value);
            }

            using (var s = store.OpenSession())
            {
                clearCache = true;
                
                var l1 = s.Query<User>().Where(x=>x.Name == "Arava").Lazily();
                var l2 = s.Query<User>().Where(x=>x.Name == "Phoebe").Lazily();
                var l3 = s.Query<User>().Where(x=>x.Name != null).CountLazily();
                
                Assert.NotEmpty(l1.Value);
                Assert.Empty(l2.Value);
                Assert.Equal(1, l3.Value);
            }
            
        }
    }
}
