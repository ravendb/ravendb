﻿using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6099 : RavenTestBase
    {
        private class Users_ByAge : AbstractIndexCreationTask<User>
        {
            public Users_ByAge()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Age
                               };
            }
        }

        [Fact]
        public void Session_PatchByIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 10 });
                    session.Store(new User { Age = 14 });
                    session.Store(new User { Age = 17 });

                    session.SaveChanges();
                }

                new Users_ByAge().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var indexName = new Users_ByAge().IndexName;
                    var query = session.Query<User>(indexName).Where(x => x.Age > 11);
                    var indexQuery = new IndexQuery
                    {
                        Query = query.ToString()
                    };

                    var operation = store.Operations.Send(new PatchByIndexOperation(indexName , indexQuery , new PatchRequest
                    {
                        Script = "this.Name = 'Patched';"
                    }));

                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    var users = session.Load<User>(new[] { "users/1-A", "users/2-A", "users/3-A" });

                    Assert.Equal(3, users.Count);
                    Assert.Null(users["users/1-A"].Name);
                    Assert.Equal("Patched", users["users/2-A"].Name);
                    Assert.Equal("Patched", users["users/3-A"].Name);
                }
            }
        }
    }
}