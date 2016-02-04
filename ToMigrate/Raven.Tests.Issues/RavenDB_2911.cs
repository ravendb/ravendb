using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Util;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2911 : RavenTestBase
    {
        public class User
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        public class Index_with_errors : AbstractIndexCreationTask<User>

        {
            public Index_with_errors()
            {
                Map = users => from u in users
                    select new
                    {
                        u.Name,
                        Foo = 1/(u.Name.Length - u.Name.Length)
                    };
            }
        }


        [Fact]
        public void Dynamic_query_should_not_use_index_with_errors()
        {
            using (var store = NewDocumentStore())
            {
                new Index_with_errors().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 101; i++) //if less than 100 - not enough attempts to determine if the index is problematic
                        session.Store(new User {Name = "Foobar" + i});
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var indexWithErrors = store.DocumentDatabase.Statistics.Indexes.First(x => x.Name == "Index/with/errors");
                Assert.Equal(true,indexWithErrors.IsInvalidIndex); //precaution

                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var query = session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "AB")
                        .ToList();

                    Assert.NotEqual("Index/with/errors",stats.IndexName);
                }
            }
        }
    }
}
