//-----------------------------------------------------------------------
// <copyright file="QueryingOnStaleIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
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

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Name1" });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    Assert.True(commands.Query(new IndexQuery()
                    {
                        Query = $"FROM INDEX '{index.IndexName}' OFFSET 0 FETCH 2"
                    }).IsStale);
                }
            }
        }
    }
}
