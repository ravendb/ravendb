//-----------------------------------------------------------------------
// <copyright file="QueryingOnStaleIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class QueryingOnStaleIndexes : RavenTestBase
    {
        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name
                               };
            }
        }

        [Fact]
        public void WillGetStaleResultWhenThereArePendingTasks()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                store.DatabaseCommands.Admin.StopIndexing();

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Name1" });
                    session.SaveChanges();
                }

                Assert.True(store.DatabaseCommands.Query(index.IndexName, new IndexQuery
                {
                    PageSize = 2,
                    Start = 0,
                }).IsStale);
            }
        }
    }
}
