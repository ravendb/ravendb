// -----------------------------------------------------------------------
//  <copyright file="DocumentReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Raven.Server.Documents.Replication;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class DocumentReplication : ReplicationTestBase
    {
        public DocumentReplication(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanReplicateDocument()
        {
            using (var source = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var destination = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {

                await SetupReplicationAsync(source, destination);
                string id;
                using (var session = source.OpenAsyncSession())
                {
                    var user = new User {Name = "Arek"};

                    await session.StoreAsync(user);

                    await session.SaveChangesAsync();

                    id = user.Id;
                }

                var fetchedUser = WaitForDocumentToReplicate<User>(destination, id, 2_000);
                Assert.NotNull(fetchedUser);

                Assert.Equal("Arek", fetchedUser.Name);
            }
        }

        [Fact]
        public async Task CanReplicateDocumentDeletion()
        {
            using (var source = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var destination = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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
            using (var source = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var destination = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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
                    var cv1 = conflicts[0].ChangeVector.ToChangeVector();
                    var cv2 = conflicts[1].ChangeVector.ToChangeVector();
                    Assert.NotEqual(cv1[0].DbId, cv2[0].DbId);
                    Assert.Equal(1, cv1[0].Etag);
                    Assert.Equal(1, cv2[0].Etag);
                }
            }
        }


        [Fact]
        public async Task ShouldCreateConflictThenResolveIt()
        {
            using (var source = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var destination = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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
                    var cv1 = conflicts[0].ChangeVector.ToChangeVector();
                    var cv2 = conflicts[1].ChangeVector.ToChangeVector();
                    Assert.NotEqual(cv1[0].DbId, cv2[0].DbId);
                    Assert.Equal(1, cv1[0].Etag);
                    Assert.Equal(1, cv2[0].Etag);
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
