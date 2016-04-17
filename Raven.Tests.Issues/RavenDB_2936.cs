// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2936.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2936 : RavenTest
    {
        [Fact]
        public void ShouldNotRetrieveOperationDetails()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Users/ByName", new IndexDefinition()
                {
                    Map = "from user in docs.Users select new { user.Name }"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Arek"
                    });

                    session.Store(new User()
                    {
                        Name = "Oren"
                    });

                    session.Store(new User()
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var result = store.DatabaseCommands.UpdateByIndex("Users/ByName",
                    new IndexQuery(),
                    new ScriptedPatchRequest
                    {
                        Script = @"this.LastName = 'Smith'"
                    })
                    .WaitForCompletion().Value<RavenJArray>("Batch");

                Assert.Empty(result);

                WaitForIndexing(store);

                result = store.DatabaseCommands.UpdateByIndex("Users/ByName",
                    new IndexQuery(),
                    new[]
                    {
                        new PatchRequest
                        {
                            Type = PatchCommandType.Set,
                            Name = "NewProp",
                            Value = "Value"
                        }
                    }).WaitForCompletion().Value<RavenJArray>("Batch");

                Assert.Empty(result);

                WaitForIndexing(store);

                result = store.DatabaseCommands.DeleteByIndex("Users/ByName", new IndexQuery())
                    .WaitForCompletion().Value<RavenJArray>("Batch");

                WaitForIndexing(store);

                Assert.Empty((RavenJArray)result);
            }
        }

        [Fact]
        public void ShouldRetrieveOperationDetailsWhenTheyWereRequested()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Users/ByName", new IndexDefinition()
                {
                    Map = "from user in docs.Users select new { user.Name }"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Arek"
                    });

                    session.Store(new User()
                    {
                        Name = "Oren"
                    });

                    session.Store(new User()
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var result = store.DatabaseCommands.UpdateByIndex("Users/ByName",
                    new IndexQuery(),
                    new ScriptedPatchRequest
                    {
                        Script = @"this.LastName = 'Smith'"
                    }, new BulkOperationOptions { RetrieveDetails = true })
                    .WaitForCompletion();

                Assert.NotNull(result);

                result = store.DatabaseCommands.UpdateByIndex("Users/ByName",
                    new IndexQuery(),
                    new[]
                    {
                        new PatchRequest
                        {
                            Type = PatchCommandType.Set,
                            Name = "NewProp",
                            Value = "Value"
                        }
                    }, new BulkOperationOptions { RetrieveDetails = true }).WaitForCompletion();

                Assert.NotNull(result);

                result = store.DatabaseCommands.DeleteByIndex("Users/ByName", new IndexQuery(), new BulkOperationOptions { RetrieveDetails = true })
                    .WaitForCompletion();

                Assert.NotNull(result);
            }
        }
    }
}
