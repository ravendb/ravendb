using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsRevisions : RavenTestBase
    {
        public AttachmentsRevisions(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReplaceAttachmentsStreamAfterRevisionsEnabled()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "image/png"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = true;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", backgroundStream, "ImGgE/jPeG"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(2, rev.Count);

                    var att1 = rev[0].GetObjects("@attachments");
                    var att2 = rev[1].GetObjects("@attachments");
                    Assert.Equal(1, att1.Length);
                    Assert.Equal(1, att2.Length);

                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var hash1 = ctx.GetLazyString("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="))
                using (var hash2 = ctx.GetLazyString("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U="))
                using (Slice.From(ctx.Allocator, hash1, out var hash1Slice))
                using (Slice.From(ctx.Allocator, hash2, out var hash2Slice))
                {
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash1));
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash2));
                    Assert.Equal(1, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).RegularHashes);
                    Assert.Equal(2, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash2Slice).RegularHashes);
                }
            }
        }

        [Fact]
        public async Task ReplaceAttachmentsStreamAndDocumentAfterRevisionsEnabled()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "image/png"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = true;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>("users/1");
                    u.Age = 30;
                    session.Advanced.Attachments.Store(u, "foo/bar", backgroundStream, "ImGgE/jPeG");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(3, rev.Count);

                    var att1 = rev[0].GetObjects("@attachments");
                    var att2 = rev[1].GetObjects("@attachments");
                    var att3 = rev[2].GetObjects("@attachments");
                    Assert.Equal(1, att1.Length);
                    Assert.Equal(1, att2.Length);
                    Assert.Equal(1, att3.Length);

                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var hash1 = ctx.GetLazyString("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="))
                using (var hash2 = ctx.GetLazyString("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U="))
                using (Slice.From(ctx.Allocator, hash1, out var hash1Slice))
                using (Slice.From(ctx.Allocator, hash2, out var hash2Slice))
                {
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash1));
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash2));
                    Assert.Equal(2, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).RegularHashes);
                    Assert.Equal(2, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash2Slice).RegularHashes);
                }
            }
        }

        [Fact]
        public async Task ModifyAttachmentsDetailsAfterRevisionsEnabled()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "image/png"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = true;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "ImGgE/jPeG"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(2, rev.Count);

                    var att1 = rev[0].GetObjects("@attachments");
                    var att2 = rev[1].GetObjects("@attachments");
                    Assert.Equal(1, att1.Length);
                    Assert.Equal(1, att2.Length);

                }
                WaitForUserToContinueTheTest(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var hash1 = ctx.GetLazyString("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="))
                using (Slice.From(ctx.Allocator, hash1, out var hash1Slice))
                {
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash1));
                    // 1 doc + 2 rev
                    Assert.Equal(3, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).RegularHashes);
                }
            }
        }

        [Fact]
        public async Task ModifyAttachmentsDetailsAndDocumentAfterRevisionsEnabled()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "image/png"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = true;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>("users/1");
                    u.Age = 30;
                    session.Advanced.Attachments.Store(u, "foo/bar", profileStream, "ImGgE/jPeG");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(3, rev.Count);

                    var att1 = rev[0].GetObjects("@attachments");
                    var att2 = rev[1].GetObjects("@attachments");
                    var att3 = rev[2].GetObjects("@attachments");
                    Assert.Equal(1, att1.Length);
                    Assert.Equal(1, att2.Length);
                    Assert.Equal(1, att3.Length);

                }
                WaitForUserToContinueTheTest(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var hash1 = ctx.GetLazyString("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="))
                using (Slice.From(ctx.Allocator, hash1, out var hash1Slice))
                {
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash1));
                    // 1 doc + 3 rev
                    Assert.Equal(4, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).RegularHashes);
                }
            }
        }

        public static Guid dbId = new Guid("00000000-48c4-421e-9466-000000000000");

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task PutAttachments(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                int? shardNumber = null;
                if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                {
                    shardNumber = await Sharding.GetShardNumberForAsync(store, "users/1");
                }

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                var names = CreateDocumentWithAttachments(store);
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertRevisionAttachments(names, 3, revisions[0], session);
                    AssertRevisionAttachments(names, 2, revisions[1], session);
                    AssertRevisionAttachments(names, 1, revisions[2], session);
                    AssertNoRevisionAttachment(revisions[3], session);
                }, 9, shardNumber: shardNumber);

                // Delete document should delete all the attachments
                store.Commands().Delete("users/1", null);
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session, true);
                    AssertRevisionAttachments(names, 3, revisions[1], session);
                    AssertRevisionAttachments(names, 2, revisions[2], session);
                    AssertRevisionAttachments(names, 1, revisions[3], session);
                }, 6, expectedCountOfDocuments: 0, shardNumber: shardNumber);

                // Create another revision which should delete old revision
                using (var session = store.OpenSession()) // This will delete the revision #1 which is without attachment
                {
                    session.Store(new User { Name = "Fitzchak 2" }, "users/1");
                    session.SaveChanges();
                }
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session, true);
                    AssertRevisionAttachments(names, 3, revisions[2], session);
                    AssertRevisionAttachments(names, 2, revisions[3], session);
                }, 5, shardNumber: shardNumber);

                using (var session = store.OpenSession()) // This will delete the revision #2 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 3" }, "users/1");
                    session.SaveChanges();
                }
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session);
                    AssertNoRevisionAttachment(revisions[2], session, true);
                    AssertRevisionAttachments(names, 3, revisions[3], session);
                }, 3, shardNumber: shardNumber);

                using (var session = store.OpenSession()) // This will delete the revision #3 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 4" }, "users/1");
                    session.SaveChanges();
                }
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session);
                    AssertNoRevisionAttachment(revisions[2], session);
                    AssertNoRevisionAttachment(revisions[3], session, true);
                }, 0, expectedCountOfUniqueAttachments: 0, shardNumber: shardNumber);

                using (var session = store.OpenSession()) // This will delete the revision #4 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 5" }, "users/1");
                    session.SaveChanges();
                }
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session);
                    AssertNoRevisionAttachment(revisions[2], session);
                    AssertNoRevisionAttachment(revisions[3], session);
                }, 0, expectedCountOfUniqueAttachments: 0, shardNumber: shardNumber);
            }
        }

        public static string[] CreateDocumentWithAttachments(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Fitzchak" }, "users/1");
                session.SaveChanges();
            }

            var names = new[]
            {
                "profile.png",
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt"
            };
            using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
            {
                var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[0], profileStream, "image/png"));
                Assert.Contains("A:3", result.ChangeVector);
                Assert.Equal(names[0], result.Name);
                Assert.Equal("users/1", result.DocumentId);
                Assert.Equal("image/png", result.ContentType);
                Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
            }
            using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
            {
                var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                Assert.Contains("A:7", result.ChangeVector);
                Assert.Equal(names[1], result.Name);
                Assert.Equal("users/1", result.DocumentId);
                Assert.Equal("ImGgE/jPeG", result.ContentType);
                Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
            }
            using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
            {
                var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                Assert.Contains("A:12", result.ChangeVector);
                Assert.Equal(names[2], result.Name);
                Assert.Equal("users/1", result.DocumentId);
                Assert.Equal("", result.ContentType);
                Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", result.Hash);
            }
            return names;
        }

        private static void AssertRevisions(DocumentStore store, string[] names, Action<IDocumentSession, List<User>> assertAction,
            long expectedCountOfAttachments, long expectedCountOfDocuments = 1, long expectedCountOfUniqueAttachments = 3, int? shardNumber = null)
        {
            store.Maintenance.ForTesting(() => new GetStatisticsOperation()).AssertAll((key, statistics) =>
            {
                if (key.ShardNumber.HasValue == false || key.ShardNumber == shardNumber)
                {
                    Assert.Equal(expectedCountOfAttachments, statistics.CountOfAttachments);
                    Assert.Equal(expectedCountOfUniqueAttachments, statistics.CountOfUniqueAttachments);
                    Assert.Equal(4, statistics.CountOfRevisionDocuments);
                    Assert.Equal(expectedCountOfDocuments, statistics.CountOfDocuments);
                    Assert.Equal(0, statistics.CountOfIndexes);
                }
            });

            using (var session = store.OpenSession())
            {
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1");
                Assert.Equal(4, revisions.Count);
                assertAction(session, revisions);
            }
        }

        private static void AssertNoRevisionAttachment(User revision, IDocumentSession session, bool isDeleteRevision = false)
        {
            var metadata = session.Advanced.GetMetadataFor(revision);
            var flags = DocumentFlags.HasRevisions | DocumentFlags.Revision;
            if (isDeleteRevision)
                flags = DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions;
            Assert.Equal(flags.ToString(), metadata[Constants.Documents.Metadata.Flags]);
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));
        }

        private static void AssertRevisionAttachments(string[] names, int expectedCount, User revision, IDocumentSession session)
        {
            var metadata = session.Advanced.GetMetadataFor(revision);
            Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.HasAttachments).ToString(), metadata[Constants.Documents.Metadata.Flags]);
            var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
            Assert.Equal(expectedCount, attachments.Length);

            var orderedNames = names.Take(expectedCount).OrderBy(x => x).ToArray();
            for (var i = 0; i < expectedCount; i++)
            {
                var name = orderedNames[i];
                var attachment = attachments[i];
                Assert.Equal(name, attachment.GetString(nameof(AttachmentName.Name)));
                var hash = attachment.GetString(nameof(AttachmentName.Hash));
                if (name == names[1])
                {
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                }
                else if (name == names[2])
                {
                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                }
                else if (name == names[0])
                {
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                }
            }

            var changeVector = session.Advanced.GetChangeVectorFor(revision);
            var readBuffer = new byte[8];
            for (var i = 0; i < names.Length; i++)
            {
                var name = names[i];
                if (orderedNames.Contains(name) == false)
                    continue;

                using (var attachmentStream = new MemoryStream(readBuffer))
                using (var attachment = session.Advanced.Attachments.GetRevision("users/1", name, changeVector))
                {
                    attachment.Stream.CopyTo(attachmentStream);
                    if (i >= expectedCount)
                    {
                        Assert.Null(attachment);
                        continue;
                    }

                    Assert.Equal(name, attachment.Details.Name);
                    if (name == names[0])
                    {
                        if (expectedCount == 1)
                            Assert.Contains("A:4", attachment.Details.ChangeVector);
                        else if (expectedCount == 2)
                            Assert.Contains("A:8", attachment.Details.ChangeVector);
                        else if (expectedCount == 3)
                            Assert.Contains("A:13", attachment.Details.ChangeVector);
                        else
                            throw new ArgumentOutOfRangeException(nameof(i));
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                        Assert.Equal("image/png", attachment.Details.ContentType);
                        Assert.Equal(3, attachmentStream.Position);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                    }
                    else if (name == names[1])
                    {
                        if (expectedCount == 2)
                            Assert.Contains("A:8", attachment.Details.ChangeVector);
                        else if (expectedCount == 3)
                            Assert.Contains("A:13", attachment.Details.ChangeVector);
                        else
                            throw new ArgumentOutOfRangeException(nameof(i));
                        Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                        Assert.Equal("ImGgE/jPeG", attachment.Details.ContentType);
                        Assert.Equal(5, attachmentStream.Position);
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", attachment.Details.Hash);
                    }
                    else if (name == names[2])
                    {
                        if (expectedCount == 3)
                            Assert.Contains("A:13", attachment.Details.ChangeVector);
                        else
                            throw new ArgumentOutOfRangeException(nameof(i));
                        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                        Assert.Equal("", attachment.Details.ContentType);
                        Assert.Equal(5, attachmentStream.Position);
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.Details.Hash);
                    }
                }
            }
        }
    }
}
