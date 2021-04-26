using System;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class BeforeStoreEventArgs : EventArgs
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

    public class AfterSaveChangesEventArgs : EventArgs
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

    public class BeforeDeleteEventArgs : EventArgs
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

    public class BeforeQueryEventArgs : EventArgs
    {
        public BeforeQueryEventArgs(InMemoryDocumentSessionOperations session, IDocumentQueryCustomization queryCustomization)
        {
            Session = session;
            QueryCustomization = queryCustomization;
        }

        public InMemoryDocumentSessionOperations Session { get; }

        public IDocumentQueryCustomization QueryCustomization { get; }
    }

    public class SessionCreatedEventArgs : EventArgs
    {
        public SessionCreatedEventArgs(InMemoryDocumentSessionOperations session)
        {
            Session = session;
        }

        public InMemoryDocumentSessionOperations Session { get; }
    }

    public class BeforeConversionToDocumentEventArgs : EventArgs
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

    public class AfterConversionToDocumentEventArgs : EventArgs
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

    public class BeforeConversionToEntityEventArgs : EventArgs
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

    public class AfterConversionToEntityEventArgs : EventArgs
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

    public class FailedRequestEventArgs : EventArgs
    {
        public string Database { get; }

        public string Url { get; }

        public Exception Exception { get; }

        internal FailedRequestEventArgs(string database, string url, Exception exception)
        {
            Database = database;
            Url = url;
            Exception = exception;
        }
    }
    public class BeforeRequestEventArgs : EventArgs
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

    public class SucceedRequestEventArgs : EventArgs
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

    public class TopologyUpdatedEventArgs : EventArgs
    {
        public Topology Topology { get; }

        internal TopologyUpdatedEventArgs(Topology topology)
        {
            Topology = topology;
        }
    }

    public class SessionDisposingEventArgs : EventArgs
    {
        public InMemoryDocumentSessionOperations Session { get; }

        internal SessionDisposingEventArgs(InMemoryDocumentSessionOperations session)
        {
            Session = session;
        }
    }
}
