//-----------------------------------------------------------------------
// <copyright file="QueryingOnStaleIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class QueryingOnStaleIndexes : RavenNewTestBase
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

                store.Admin.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Name1" });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    Assert.True(commands.Query(index.IndexName, new IndexQuery(store.Conventions)
                    {
                        PageSize = 2,
                        Start = 0,
                    }).IsStale);
                }
            }
        }
    }
}
