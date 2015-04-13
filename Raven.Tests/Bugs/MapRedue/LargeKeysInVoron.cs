using Raven.Abstractions.Indexing;
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
            public string[] Aliases { get; set; }

            public User()
            {
                this.Aliases = new string[] { };
            }
        }

        public class LargeKeysInVoronFunction : AbstractIndexCreationTask<User, LargeKeysInVoronFunction.ReduceResult>
        {
            public class ReduceResult
            {
                public string Name { get; set; }
                public int Count { get; set; }
                public AliasReduceResult[] Aliases { get; set; }

	            public class AliasReduceResult
	            {
		            public string Name;
		            public string Alias;
	            }
            }


            public LargeKeysInVoronFunction()
            {
                Map = users => from user in users
                               let aliases = from alias in user.Aliases
                                             select new
                                             {
                                                 user.Name,
                                                 Alias = alias
                                             }
                               from alias in aliases
                               group aliases by new
                                {
                                    Name = alias.Name,
                                } into g
                                select new
                                {
                                    Name = g.Key.Name,                                    
                                    Count = g.Count(),
                                    Aliases = g
                                };

                Reduce = users => from user in users
                                  group user by new
                                    {
                                        Name = user.Name,
                                        Count = user.Count,
                                    } into g
                                  select new ReduceResult
                                    {
                                        // This is the bag used on the reduce.
                                        Name = g.Key.Name,
                                        Count = g.Key.Count,
                                        Aliases = g.First().Aliases
                                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }



        [Fact]
        public void CanHandleLargeReduceKeys()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "name/ayende", Name = new string('A', 10000), Aliases = new[] { "alias1", "alias2" } });
                    session.Store(new User { Id = "name/ayende2", Name = new string('A', 10000), Aliases = new[]{ "alias1", "alias3"} });
                    session.SaveChanges();
                }

                new LargeKeysInVoronFunction().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<LargeKeysInVoronFunction.ReduceResult, LargeKeysInVoronFunction>()
                                       .ToList();

                    Assert.Equal(1, query.Count());

                    var result = query.First();
                    Assert.Equal(2, result.Count);
                }

                foreach (var error in store.SystemDatabase.Statistics.Errors)
                    Console.WriteLine(error.ToString());

                Assert.Empty(store.SystemDatabase.Statistics.Errors);
            }
        }
    }
}
