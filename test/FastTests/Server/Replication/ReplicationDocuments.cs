// -----------------------------------------------------------------------
//  <copyright file="DocumentReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Replication
{
    public class DocumentReplication : ReplicationTestsBase
    {
        [Fact]
        public async Task CanReplicateDocument()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {

                await SetupReplicationAsync(source, destination);
                string id;
                using (var session = source.OpenAsyncSession())
                {
                    var user = new User {Name = "Arek"};

                    await session.StoreAsync(user);

                    await session.SaveChangesAsync();

                    id = user.Id;
                    //TODO : uncomment this when the topology endpoint is implemented
                    //await source.Replication.WaitAsync(etag: session.Advanced.GetEtagFor(user));
                }

                var fetchedUser = WaitForDocumentToReplicate<User>(destination, id, 2_000);
                Assert.NotNull(fetchedUser);

                Assert.Equal("Arek", fetchedUser.Name);
            }
        }

        [Fact]
        public async Task CanReplicateDocumentDeletion()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                await SetupReplicationAsync(source, destination);

                using (var sourceCommands = source.Commands())
                {
                    sourceCommands.Put("docs/1", null, new { Key = "Value" }, null);

                    Assert.True(WaitForDocument(destination, "docs/1"));

                    sourceCommands.Delete("docs/1", null);
                }

                using (var destinationCommands = destination.Commands())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (destinationCommands.Get("docs/1") == null)
                            break;
                        Thread.Sleep(100);
                    }

                    Assert.Null(destinationCommands.Get("docs/1"));
                }
            }
        }

        [Fact]
        public async Task GetConflictsResult_command_should_work_properly()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                using (var sourceCommands = source.Commands())
                using (var destinationCommands = destination.Commands())
                {
                    sourceCommands.Put("docs/1", null, new {Key = "Value"}, null);
                    destinationCommands.Put("docs/1", null, new {Key = "Value2"}, null);

                    await SetupReplicationAsync(source, destination);

                    sourceCommands.Put("marker", null, new {Key = "Value"}, null);

                    Assert.True(WaitForDocument(destination, "marker"));

                    var conflicts = destination.Commands().GetConflictsFor("docs/1");
                    Assert.Equal(2, conflicts.Length);
                    Assert.NotEqual(conflicts[0].ChangeVector[0].DbId, conflicts[1].ChangeVector[0].DbId);
                    Assert.Equal(1, conflicts[0].ChangeVector[0].Etag);
                    Assert.Equal(1, conflicts[1].ChangeVector[0].Etag);
                }
            }
        }


        [Fact]
        public async Task ShouldCreateConflictThenResolveIt()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                GetConflictsResult.Conflict[] conflicts;
                using (var sourceCommands = source.Commands())
                using (var destinationCommands = destination.Commands())
                {
                    sourceCommands.Put("docs/1", null, new { Key = "Value" }, null);
                    destinationCommands.Put("docs/1", null, new { Key = "Value2" }, null);

                    await SetupReplicationAsync(source, destination);

                    sourceCommands.Put("marker", null, new { Key = "Value" }, null);

                    Assert.True(WaitForDocument(destination, "marker"));

                    conflicts = destination.Commands().GetConflictsFor("docs/1");
                    Assert.Equal(2, conflicts.Length);
                    Assert.NotEqual(conflicts[0].ChangeVector[0].DbId, conflicts[1].ChangeVector[0].DbId);
                    Assert.Equal(1, conflicts[0].ChangeVector[0].Etag);
                    Assert.Equal(1, conflicts[1].ChangeVector[0].Etag);
                }

                Assert.Throws<DocumentConflictException>(() => destination.Commands().Get("docs/1"));
                //now actually resolve the conflict
                //(resolve by using first variant)
                var resolution = conflicts[0];
                destination.Commands().Put("docs/1", null, resolution.Doc);

                ////this shouldn't throw since we have just resolved the conflict
                var fetchedDoc = destination.Commands().Get("docs/1");
                var actualVal = resolution.Doc["Key"] as LazyStringValue;
                var fetchedVal = fetchedDoc["Key"] as LazyStringValue;

                ////not null asserts -> precaution
                Assert.NotNull(actualVal); 
                Assert.NotNull(fetchedVal);

                Assert.Equal(fetchedVal.ToString(), actualVal.ToString());
            }
        }
    }
}
