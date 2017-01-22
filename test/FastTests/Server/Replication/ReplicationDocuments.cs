// -----------------------------------------------------------------------
//  <copyright file="DocumentReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Exceptions;
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
                SetupReplication(source, destination);
                string id;
                using (var session = source.OpenAsyncSession())
                {
                    var user = new User { Name = "Arek" };

                    await session.StoreAsync(user);

                    await session.SaveChangesAsync();

                    id = user.Id;
                    //TODO : uncomment this when the topology endpoint is implemented
                    //await source.Replication.WaitAsync(etag: session.Advanced.GetEtagFor(user));
                }

                var fetchedUser = WaitForDocumentToReplicate<User>(destination, id, 2000);
                Assert.NotNull(fetchedUser);

                Assert.Equal("Arek", fetchedUser.Name);
            }
        }

        [Fact]
        public void CanReplicateDocumentDeletion()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                SetupReplication(source, destination);

                using (var sourceCommands = source.Commands())
                {
                    sourceCommands.Put("docs/1", null, new { Key = "Value" }, null);

                    var document = WaitForDocument(destination, "docs/1");

                    Assert.NotNull(document);

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

        [Fact(Skip = "Client API needs to support changed protocol of replication conflicts before this would work")]
        public void ShouldCreateConflictThenResolveIt()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                using (var sourceCommands = source.Commands())
                using (var destinationCommands = destination.Commands())
                {
                    sourceCommands.Put("docs/1", null, new { Key = "Value" }, null);
                    destinationCommands.Put("docs/1", null, new { Key = "Value2" }, null);

                    SetupReplication(source, destination);

                    sourceCommands.Put("marker", null, new { Key = "Value" }, null);

                    var marker = WaitForDocument(destination, "marker");

                    Assert.NotNull(marker);

                    var conflicts = GetConflicts(destination, "docs/1");
                    Assert.Equal(2, conflicts["docs/1"].Count);
                    Assert.NotEqual(conflicts["docs/1"][0][0].DbId, conflicts["docs/1"][1][0].DbId);
                    Assert.Equal(1, conflicts["docs/1"][0][0].Etag);
                    Assert.Equal(1, conflicts["docs/1"][1][0].Etag);

                    var conflictException = Assert.Throws<ConflictException>(() => destinationCommands.Get("docs/1"));
                    Assert.Equal("Conflict detected on docs/1, conflict must be resolved before the document will be accessible", conflictException.Message);
                }
                // resolve by using first
                //TODO : when client API is finished, refactor this so the test works as designed
                //var resolution = destination.DatabaseCommands.Get(conflictException.ConflictedVersionIds[0]);
                //resolution.Metadata.Remove(Constants.Replication.RavenReplicationConflictDocument);
                //destination.DatabaseCommands.Put("docs/1", null, resolution.DataAsJson, resolution.Metadata);

                //destination.DatabaseCommands.GetAttachment("docs/1");
            }
        }
    }
}
