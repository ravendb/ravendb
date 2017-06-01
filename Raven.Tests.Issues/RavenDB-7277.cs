// -----------------------------------------------------------------------
//  <copyright file="RavenDB-7277.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Tests.Core.Replication;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_7277 : RavenReplicationCoreTest
    {
        [Fact]
        public async Task correct_document_count_after_conflict_resolve1()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                using (var session = source.OpenAsyncSession())
                {
                    var user = new User { Name = "Grisha" };
                    await session.StoreAsync(user);
                    var user2 = new User { Name = "Grisha Kotler" };
                    await session.StoreAsync(user2);
                    await session.SaveChangesAsync();
                }

                using (var session = destination.OpenAsyncSession())
                {
                    var user = new User { Name = "Grisha" };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();

                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                SetConflictResolutionFor(destination);
                SetupReplication(source, destinations: destination);

                // make sure that the the "users/1" document was replicated
                WaitForDocument<User>(destination, "users/2");

                using (var session = destination.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.Null(user1);

                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);

                    var systemDocs = session.Advanced.LoadStartingWith<dynamic>("Raven/");
                    var totalDocs = destination.DatabaseCommands.GetStatistics().CountOfDocuments;

                    // there should be only one document except the system documents
                    Assert.Equal(1, totalDocs - systemDocs.Length);
                }
            }
        }

        [Fact]
        public async Task correct_document_count_after_conflict_resolve2()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                using (var session = source.OpenAsyncSession())
                {
                    var user = new User { Name = "Grisha" };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                using (var session = destination.OpenAsyncSession())
                {
                    var user = new User { Name = "Grisha" };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();

                    session.Delete("users/1");
                    var user2 = new User { Name = "Grisha Kotler" };
                    await session.StoreAsync(user2, "users/2");
                    await session.SaveChangesAsync();
                }

                SetConflictResolutionFor(source);
                SetupReplication(destination, destinations: source);

                // make sure that the the "users/1" document was replicated
                WaitForDocument<User>(source, "users/2");

                using (var session = source.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.Null(user1);

                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);

                    var systemDocs = session.Advanced.LoadStartingWith<dynamic>("Raven/");
                    var totalDocs = source.DatabaseCommands.GetStatistics().CountOfDocuments;

                    // there should be only one document except the system documents
                    Assert.Equal(1, totalDocs - systemDocs.Length);
                }
            }
        }

        private static void SetConflictResolutionFor(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new ReplicationConfig
                {
                    DocumentConflictResolution = StraightforwardConflictResolution.ResolveToLatest
                }, Constants.RavenReplicationConfig);

                session.SaveChanges();
            }
        }
    }
}
