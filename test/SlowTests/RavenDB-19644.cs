using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_19644 : RavenTestBase
    {
        private const string AttachmentName = "Attachment";
        private readonly TimeSpan _timeout = TimeSpan.FromMilliseconds(100);

        public RavenDB_19644(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WaitForIndexesAfterSaveChangesSupportsPut()
        {
            using var store = GetDocumentStore();
            new AttachmentIndex().Execute(store);
            var attachmentDocument = new AttachmentDocument("");
            using (var session = store.OpenSession())
            {
                session.Store(attachmentDocument);
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            store.Maintenance.Send(new StopIndexingOperation());

            using (var session = store.OpenSession())
            {
                using var memoryStream = new MemoryStream(Encodings.Utf8.GetBytes("Maciej"));
                session.Advanced.Attachments.Store(attachmentDocument.Id, AttachmentName, memoryStream, "txt");
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: _timeout);
                AssertIndexIsStale(session);
            }
        }

        private static void AssertIndexIsStale(IDocumentSession session)
        {
            var exception = Assert.Throws<RavenTimeoutException>(() => session!.SaveChanges());
            Assert.Contains("could not verify that all indexes has caught up with the changes as of etag", exception.ToString());
        }

        [Fact]
        public void WaitForIndexesAfterSaveChangesSupportsCopy()
        {
            using var store = GetDocumentStore();
            new AttachmentIndex().Execute(store);
            var sourceDocument = new AttachmentDocument("");
            var destinationDocument = new AttachmentDocument("");

            using (var session = store.OpenSession())
            {
                session.Store(sourceDocument);
                session.Store(destinationDocument);
                using var memoryStream = new MemoryStream(Encodings.Utf8.GetBytes("Maciej"));
                session.Advanced.Attachments.Store(sourceDocument.Id, AttachmentName, memoryStream, "txt");
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            store.Maintenance.Send(new StopIndexingOperation());

            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Copy(sourceDocument.Id, AttachmentName, destinationDocument.Id, AttachmentName);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: _timeout);
                AssertIndexIsStale(session);
            }
        }

        [Fact]
        public void WaitForIndexesAfterSaveChangesSupportsMove()
        {
            using var store = GetDocumentStore();
            new AttachmentIndex().Execute(store);
            var sourceDocument = new AttachmentDocument("");
            var destinationDocument = new AttachmentDocument("");

            using (var session = store.OpenSession())
            {
                session.Store(sourceDocument);
                session.Store(destinationDocument);
                using var memoryStream = new MemoryStream(Encodings.Utf8.GetBytes("Maciej"));
                session.Advanced.Attachments.Store(sourceDocument.Id, AttachmentName, memoryStream, "txt");
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            store.Maintenance.Send(new StopIndexingOperation());

            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Move(sourceDocument.Id, AttachmentName, destinationDocument.Id, AttachmentName);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: _timeout);
                AssertIndexIsStale(session);
            }
        }

        [Fact]
        public void WaitForIndexesAfterSaveChangesSupportsCopyToDifferentCollection()
        {
            using var store = GetDocumentStore();
            var sourceIndex = new AttachmentIndex();
            sourceIndex.Execute(store);

            var destinationIndex = new DestinationAttachmentIndex();
            destinationIndex.Execute(store);

            var srcDocument = new AttachmentDocument("");
            var dstDocument = new AttachmentDestinationDocument("");

            using (var session = store.OpenSession())
            {
                session.Store(srcDocument);
                session.Store(dstDocument);
                using var memoryStream = new MemoryStream(Encodings.Utf8.GetBytes("Maciej"));
                session.Advanced.Attachments.Store(srcDocument.Id, AttachmentName, memoryStream, "txt");
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            store.Maintenance.Send(new StopIndexOperation(destinationIndex.IndexName));

            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Copy(srcDocument.Id, AttachmentName, dstDocument.Id, AttachmentName);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: _timeout);
                AssertIndexIsStale(session);
            }
        }

        [Theory]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(false, false)]
        public void WaitForIndexesAfterSaveChangesSupportsMoveToDifferentCollection(bool sourceActive, bool destinationActive)
        {
            using var store = GetDocumentStore();
            var sourceIndex = new AttachmentIndex();
            sourceIndex.Execute(store);

            var destinationIndex = new DestinationAttachmentIndex();
            destinationIndex.Execute(store);

            var srcDocument = new AttachmentDocument("");
            var dstDocument = new AttachmentDestinationDocument("");

            using (var session = store.OpenSession())
            {
                session.Store(srcDocument);
                session.Store(dstDocument);
                using var memoryStream = new MemoryStream(Encodings.Utf8.GetBytes("Maciej"));
                session.Advanced.Attachments.Store(srcDocument.Id, AttachmentName, memoryStream, "txt");
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            if (sourceActive == false)
                store.Maintenance.Send(new StopIndexOperation(sourceIndex.IndexName));

            if (destinationActive == false)
                store.Maintenance.Send(new StopIndexOperation(destinationIndex.IndexName));

            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Move(srcDocument.Id, AttachmentName, dstDocument.Id, AttachmentName);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: _timeout);
                AssertIndexIsStale(session);
            }
        }

        [Fact]
        public void WaitForIndexesAfterSaveChangesSupportsDelete()
        {
            using var store = GetDocumentStore();
            new AttachmentIndex().Execute(store);
            var attachmentDocument = new AttachmentDocument("");
            using (var session = store.OpenSession())
            {
                session.Store(attachmentDocument);
                using var memoryStream = new MemoryStream(Encodings.Utf8.GetBytes("Maciej"));
                session.Advanced.Attachments.Store(attachmentDocument.Id, AttachmentName, memoryStream, "txt");
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            store.Maintenance.Send(new StopIndexingOperation());
            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Delete(attachmentDocument.Id, AttachmentName);
                session.Advanced.WaitForIndexesAfterSaveChanges(timeout: _timeout);
                AssertIndexIsStale(session);
            }
        }

        private record AttachmentDocument(string Name, string Id = null);

        private record AttachmentDestinationDocument(string Name, string Id = null);

        private class AttachmentIndex : AbstractIndexCreationTask<AttachmentDocument>
        {
            public AttachmentIndex()
            {
                Map = documents => from doc in documents
                    let attachments = LoadAttachment(doc, AttachmentName).GetContentAsString() ?? ""
                    select new {Name = attachments};
            }
        }

        private class DestinationAttachmentIndex : AbstractIndexCreationTask<AttachmentDestinationDocument>
        {
            public DestinationAttachmentIndex()
            {
                Map = documents => from doc in documents
                    let attachments = LoadAttachment(doc, AttachmentName).GetContentAsString() ?? ""
                    select new {Name = attachments};
            }
        }
    }
}
