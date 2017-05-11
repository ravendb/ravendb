using FastTests;
using FastTests.Client.Attachments;
using Microsoft.AspNetCore.Http.Features;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsBigFiles : RavenTestBase
    {
        // TODO: This should be a failing test, but it passing.
        [Fact]
        public void AttachmentBiggerThan128Mb_WhichIsMaxMultipartBodyLengthLimit()
        {
            var size = FormOptions.DefaultMultipartBodyLengthLimit * 2;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                using (var stream = new BigDummyStream(size))
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");

                    session.Advanced.StoreAttachment(user, "256mb-file", stream);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    using (var bigStream = new BigDummyStream(size))
                    {
                        var attachment = session.Advanced.GetAttachment(user, "256mb-file", (result, stream) => stream.CopyTo(bigStream));
                        Assert.Equal(2, attachment.Etag);
                        Assert.Equal("256mb-file", attachment.Name);
                        Assert.Equal("G/VBSDnFqmLKAphJbokRdiXpfeRMcTwz", attachment.Hash);
                        Assert.Equal(size, bigStream.Position);
                        Assert.Equal(size, attachment.Size);
                        Assert.Equal("", attachment.ContentType);
                    }
                }
            }
        }

        [Theory(Skip = "TODO: Huge file")]
        [InlineData(int.MaxValue, "todoB3EIIB2gNVjsXTCD1aXlTgzuEz50")]
        public void SupportHugeAttachment_MaxLong(long size, string hash)
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
                    {
                        var attachment = session.Advanced.GetAttachment(user, "huge-file", (result, stream) => stream.CopyTo(bigStream));
                        Assert.Equal(2, attachment.Etag);
                        Assert.Equal("huge-file", attachment.Name);
                        Assert.Equal(hash, attachment.Hash);
                        Assert.Equal(size, bigStream.Position);
                        Assert.Equal(size, attachment.Size);
                        Assert.Equal("", attachment.ContentType);
                    }
                }
            }
        }
    }
}