using FastTests;
using FastTests.Client.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsBigFiles : RavenTestBase
    {
        [Theory]
        [InlineData(10, "i1enlqXQfLBMwWFN/CrLP3PtxxLX9DNhnKO75muxX0k=")]
        public void BatchRequestWithLongMultiPartSections(long size, string hash)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                using (var stream = new BigDummyStream(size))
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");

                    session.Advanced.StoreAttachment(user, "big-file", stream);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    using (var bigStream = new BigDummyStream(size))
                    using (var attachment = session.Advanced.GetAttachment(user, "big-file"))
                    {
                        attachment.Stream.CopyTo(bigStream);
                        Assert.Equal(2, attachment.Details.Etag);
                        Assert.Equal("big-file", attachment.Details.Name);
                        Assert.Equal(hash, attachment.Details.Hash);
                        Assert.Equal(size, bigStream.Position);
                        Assert.Equal(size, attachment.Details.Size);
                        Assert.Equal("", attachment.Details.ContentType);
                    }
                }
            }
        }

        [Theory]
        [InlineData(10, "i1enlqXQfLBMwWFN/CrLP3PtxxLX9DNhnKO75muxX0k=")]
        public void SupportHugeAttachment(long size, string hash)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Fitzchak"}, "users/1");
                    session.SaveChanges();
                }

                using (var bigStream = new BigDummyStream(size))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "huge-file", bigStream));
                    Assert.Equal(2, result.Etag);
                    Assert.Equal("huge-file", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal(hash, result.Hash);
                    Assert.Equal(size, result.Size);
                    Assert.Equal(size, bigStream.Position);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    using (var bigStream = new BigDummyStream(size))
                    using (var attachment = session.Advanced.GetAttachment(user, "huge-file"))
                    {
                        attachment.Stream.CopyTo(bigStream);
                        Assert.Equal(2, attachment.Details.Etag);
                        Assert.Equal("huge-file", attachment.Details.Name);
                        Assert.Equal(hash, attachment.Details.Hash);
                        Assert.Equal(size, bigStream.Position);
                        Assert.Equal(size, attachment.Details.Size);
                        Assert.Equal("", attachment.Details.ContentType);
                    }
                }
            }
        }
    }
}