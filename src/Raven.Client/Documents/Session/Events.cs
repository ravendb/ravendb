using System;
using System.Net.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public sealed class BeforeStoreEventArgs : EventArgs
    {
        private IMetadataDictionary _documentMetadata;

        public BeforeStoreEventArgs(InMemoryDocumentSessionOperations session, string documentId, object entity)
        {
            Session = session;
            DocumentId = documentId;
            Entity = entity;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public string DocumentId { get; }
        public object Entity { get; }

        internal bool MetadataAccessed => _documentMetadata != null;

        public IMetadataDictionary DocumentMetadata => _documentMetadata ?? (_documentMetadata = Session.GetMetadataFor(Entity));
    }

    public sealed class AfterSaveChangesEventArgs : EventArgs
    {
        private IMetadataDictionary _documentMetadata;

        public AfterSaveChangesEventArgs(InMemoryDocumentSessionOperations session, string documentId, object entity)
        {
            Session = session;
            DocumentId = documentId;
            Entity = entity;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public string DocumentId { get; }
        public object Entity { get; }

        public IMetadataDictionary DocumentMetadata => _documentMetadata ?? (_documentMetadata = Session.GetMetadataFor(Entity));
    }

    public sealed class BeforeDeleteEventArgs : EventArgs
    {
        private IMetadataDictionary _documentMetadata;

        public BeforeDeleteEventArgs(InMemoryDocumentSessionOperations session, string documentId, object entity)
        {
            Session = session;
            DocumentId = documentId;
            Entity = entity;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public string DocumentId { get; }
        public object Entity { get; }

        public IMetadataDictionary DocumentMetadata => _documentMetadata ?? (_documentMetadata = Session.GetMetadataFor(Entity));
    }

    public sealed class BeforeQueryEventArgs : EventArgs
    {
        public BeforeQueryEventArgs(InMemoryDocumentSessionOperations session, IDocumentQueryCustomization queryCustomization)
        {
            Session = session;
            QueryCustomization = queryCustomization;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public IDocumentQueryCustomization QueryCustomization { get; }
    }

    public sealed class SessionCreatedEventArgs : EventArgs
    {
        public SessionCreatedEventArgs(InMemoryDocumentSessionOperations session)
        {
            Session = session;
        }

        public InMemoryDocumentSessionOperations Session { get; }
    }

    public sealed class BeforeConversionToDocumentEventArgs : EventArgs
    {
        public string Id { get; }

        public object Entity { get; }

        public InMemoryDocumentSessionOperations Session { get; }

        public BeforeConversionToDocumentEventArgs(InMemoryDocumentSessionOperations session, string id, object entity)
        {
            Session = session;
            Id = id;
            Entity = entity;
        }
    }

    public sealed class AfterConversionToDocumentEventArgs : EventArgs
    {
        public string Id { get; }

        public object Entity { get; }

        public BlittableJsonReaderObject Document { get; set; }

        public InMemoryDocumentSessionOperations Session { get; }

        public AfterConversionToDocumentEventArgs(InMemoryDocumentSessionOperations session, string id, object entity, BlittableJsonReaderObject document)
        {
            Session = session;
            Id = id;
            Entity = entity;
            Document = document;
        }
    }

    public sealed class BeforeConversionToEntityEventArgs : EventArgs
    {
        public string Id { get; }

        public Type Type { get; }

        public BlittableJsonReaderObject Document { get; set; }

        public InMemoryDocumentSessionOperations Session { get; }

        internal BeforeConversionToEntityEventArgs(InMemoryDocumentSessionOperations session, string id, Type type, BlittableJsonReaderObject document)
        {
            Session = session;
            Id = id;
            Type = type;
            Document = document;
        }
    }

    public sealed class AfterConversionToEntityEventArgs : EventArgs
    {
        public string Id { get; }

        public BlittableJsonReaderObject Document { get; }

        public object Entity { get; }

        public InMemoryDocumentSessionOperations Session { get; }

        internal AfterConversionToEntityEventArgs(InMemoryDocumentSessionOperations session, string id, BlittableJsonReaderObject document, object entity)
        {
            Session = session;
            Id = id;
            Document = document;
            Entity = entity;
        }
    }

    public sealed class FailedRequestEventArgs : EventArgs
    {
        public string Database { get; }

        public string Url { get; }
        public Exception Exception { get; }

        public HttpResponseMessage Response { get; }

        public HttpRequestMessage Request { get; }

        internal FailedRequestEventArgs(string database, string url, Exception exception)
        {
            Database = database;
            Url = url;
            Exception = exception;
        }

        internal FailedRequestEventArgs(string database, string url, Exception exception, HttpResponseMessage response = null, HttpRequestMessage request = null) : this(database, url, exception)
        {
            Response = response;
            Request = request;
        }
    }
    public sealed class BeforeRequestEventArgs : EventArgs
    {
        public string Database { get; }
        public string Url { get; }
        public HttpRequestMessage Request { get; }
        public int AttemptNumber { get; }

        internal BeforeRequestEventArgs(string database, string url, HttpRequestMessage request, int attemptNumber)
        {
            Database = database;
            Url = url;
            Request = request;
            AttemptNumber = attemptNumber;
        }
    }

    public sealed class SucceedRequestEventArgs : EventArgs
    {
        public string Database { get; }
        public string Url { get; }
        public HttpResponseMessage Response { get; }
        public HttpRequestMessage Request { get; }
        public int AttemptNumber { get; }

        internal SucceedRequestEventArgs(string database, string url, HttpResponseMessage response, HttpRequestMessage request, int attemptNumber)
        {
            Database = database;
            Url = url;
            Response = response;
            Request = request;
            AttemptNumber = attemptNumber;
        }
    }

    public sealed class TopologyUpdatedEventArgs : EventArgs
    {
        public Topology Topology { get; }

        public string Reason { get; }

        internal TopologyUpdatedEventArgs(Topology topology, string reason)
        {
            Topology = topology;
            Reason = reason;
        }
    }

    public sealed class SessionDisposingEventArgs : EventArgs
    {
        public InMemoryDocumentSessionOperations Session { get; }

        internal SessionDisposingEventArgs(InMemoryDocumentSessionOperations session)
        {
            Session = session;
        }
    }

    public sealed class BulkInsertOnProgressEventArgs : EventArgs
    {
        public BulkInsertProgress Progress { get; }
        
        internal BulkInsertOnProgressEventArgs(BulkInsertProgress progress)
        {
            Progress = progress;
        }
    }
}
