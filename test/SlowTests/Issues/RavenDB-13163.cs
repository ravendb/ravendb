using System.IO;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13163 : RavenTestBase
    {
        public RavenDB_13163(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrowOnDocumentIdTooBig()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var longId = new string('z', DocumentIdWorker.MaxIdSize);

                    session.Store(new User(), longId);

                    // should not throw
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var longerId = new string('z', DocumentIdWorker.MaxIdSize + 1);

                    session.Store(new User(), longerId);

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains($"Document ID cannot exceed {DocumentIdWorker.MaxIdSize} bytes", ex.Message);
                }

            }

        }

        [Fact]
        public void ShouldThrowOnCounterNameTooBig()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/1";
                var longName = new string('z', DocumentIdWorker.MaxIdSize - docId.Length);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), docId);
                    session.CountersFor(docId).Increment(longName);

                    // should not throw
                    session.SaveChanges();
                }

                var longerName = new string('z', DocumentIdWorker.MaxIdSize + 1);

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment(longerName);

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());

                    Assert.Contains($"Counter name cannot exceed {DocumentIdWorker.MaxIdSize} bytes", ex.Message);
                }

            }

        }

        [Fact]
        public void ShouldThrowOnAttachmentNameTooBig()
        {
            using (var store = GetDocumentStore())
            {
                var longName = new string('z', DocumentIdWorker.MaxIdSize + 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();


                }

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    Assert.Throws<RavenException>(() =>
                        store.Operations.Send(new PutAttachmentOperation("users/1", longName, stream)));
                }
            }

        }
    }
}
