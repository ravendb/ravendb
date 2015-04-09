using Raven.Client.Indexes;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Bugs.MapRedue
{
    public class LargeKeysInVoron : RavenTest
    {
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class LargeKeysInVoronFunction : AbstractIndexCreationTask<User, LargeKeysInVoronFunction.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }

            public LargeKeysInVoronFunction()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Id,
                                   user.Name
                               };

                Reduce = results => from result in results
                                    group result by result.Id
                                        into g
                                        let dummy = g.FirstOrDefault(x => x.Name != null)
                                        select new
                                        {
                                            Id = g.Key,
                                            Name = dummy.Name
                                        };
            }
        }



        [Fact]
        public void CanHandleLargeReduceKeys()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = new string('A', 10000), Name = "Ayende Rahien" });
                    session.Store(new User { Id = new string('B', 10000), Name = "Daniel Lang" });
                    session.SaveChanges();
                }

                new LargeKeysInVoronFunction().Execute(store);

                WaitForIndexing(store);

                Assert.Empty(store.SystemDatabase.Statistics.Errors);
            }
        }
    }
}
