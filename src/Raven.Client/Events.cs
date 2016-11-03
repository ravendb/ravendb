using System;
using System.Collections.Generic;
using Raven.Client.Documents;

namespace Raven.Client
{
    public class BeforeStoreEventArgs : EventArgs
    {
        private IDictionary<string, string> _documentMetadata;

        public BeforeStoreEventArgs(InMemoryDocumentSessionOperations session, string documentId, object entity)
        {
            Session = session;
            DocumentId = documentId;
            Entity = entity;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public string DocumentId { get; }
        public object Entity { get; }

        public IDictionary<string, string> DocumentMetadata => _documentMetadata ?? (_documentMetadata = Session.GetMetadataFor(Entity));
    }

    public class AfterStoreEventArgs : EventArgs
    {
        private IDictionary<string, string> _documentMetadata;

        public AfterStoreEventArgs(InMemoryDocumentSessionOperations session, string documentId, object entity)
        {
            Session = session;
            DocumentId = documentId;
            Entity = entity;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public string DocumentId { get; }
        public object Entity { get; }

        public IDictionary<string, string> DocumentMetadata => _documentMetadata ?? (_documentMetadata = Session.GetMetadataFor(Entity));
    }

    public class BeforeDeleteEventArgs : EventArgs
    {
        private IDictionary<string, string> _documentMetadata;

        public BeforeDeleteEventArgs(InMemoryDocumentSessionOperations session, string documentId, object entity)
        {
            Session = session;
            DocumentId = documentId;
            Entity = entity;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public string DocumentId { get; }
        public object Entity { get; }

        public IDictionary<string, string> DocumentMetadata => _documentMetadata ?? (_documentMetadata = Session.GetMetadataFor(Entity));
    }

    public class BeforeQueryExecutedEventArgs : EventArgs
    {
        public BeforeQueryExecutedEventArgs(InMemoryDocumentSessionOperations session, Documents.IDocumentQueryCustomization queryCustomization)
        {
            Session = session;
            QueryCustomization = queryCustomization;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public Documents.IDocumentQueryCustomization QueryCustomization { get; }
    }
}