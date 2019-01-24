using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsRevisions : RavenTestBase
    {
        public static Guid dbId = new Guid("00000000-48c4-421e-9466-000000000000");
        [Fact]
        public async Task PutAttachments()
        {
            using (var store = GetDocumentStore())
            {
                await SetDatabaseId(store, dbId);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration =>
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
                }, 9);

                // Delete document should delete all the attachments
                store.Commands().Delete("users/1", null);
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session, true);
                    AssertRevisionAttachments(names, 3, revisions[1], session);
                    AssertRevisionAttachments(names, 2, revisions[2], session);
                    AssertRevisionAttachments(names, 1, revisions[3], session);
                }, 6, expectedCountOfDocuments: 0);

                // Create another revision which should delete old revision
                using (var session = store.OpenSession()) // This will delete the revision #1 which is without attachment
                {
                    session.Store(new User {Name = "Fitzchak 2"}, "users/1");
                    session.SaveChanges();
                }
                AssertRevisions(store, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session, true);
                    AssertRevisionAttachments(names, 3, revisions[2], session);
                    AssertRevisionAttachments(names, 2, revisions[3], session);
                }, 5);

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
                }, 3);

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
                }, 0, expectedCountOfUniqueAttachments: 0);

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
                }, 0, expectedCountOfUniqueAttachments: 0);
            }
        }

        public static string[] CreateDocumentWithAttachments(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User {Name = "Fitzchak"}, "users/1");
                session.SaveChanges();
            }

            var names = new[]
            {
                "profile.png",
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt"
            };
            using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
            {
                var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[0], profileStream, "image/png"));
                Assert.Contains("A:3", result.ChangeVector);
                Assert.Equal(names[0], result.Name);
                Assert.Equal("users/1", result.DocumentId);
                Assert.Equal("image/png", result.ContentType);
                Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
            }
            using (var backgroundStream = new MemoryStream(new byte[] {10, 20, 30, 40, 50}))
            {
                var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                Assert.Contains("A:7", result.ChangeVector);
                Assert.Equal(names[1], result.Name);
                Assert.Equal("users/1", result.DocumentId);
                Assert.Equal("ImGgE/jPeG", result.ContentType);
                Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
            }
            using (var fileStream = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
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
            long expectedCountOfAttachments, long expectedCountOfDocuments = 1, long expectedCountOfUniqueAttachments = 3)
        {
            var statistics = store.Maintenance.Send(new GetStatisticsOperation());
            Assert.Equal(expectedCountOfAttachments, statistics.CountOfAttachments);
            Assert.Equal(expectedCountOfUniqueAttachments, statistics.CountOfUniqueAttachments);
            Assert.Equal(4, statistics.CountOfRevisionDocuments);
            Assert.Equal(expectedCountOfDocuments, statistics.CountOfDocuments);
            Assert.Equal(0, statistics.CountOfIndexes);

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
                        Assert.Equal(new byte[] {1, 2, 3}, readBuffer.Take(3));
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
                        Assert.Equal(new byte[] {10, 20, 30, 40, 50}, readBuffer.Take(5));
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
                        Assert.Equal(new byte[] {1, 2, 3, 4, 5}, readBuffer.Take(5));
                        Assert.Equal("", attachment.Details.ContentType);
                        Assert.Equal(5, attachmentStream.Position);
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.Details.Hash);
                    }
                }
            }
        }
    }
}
