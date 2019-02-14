using System;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerDestination
    {
        IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion);
        IDatabaseRecordActions DatabaseRecord();
        IDocumentActions Documents();
        IDocumentActions RevisionDocuments();
        IDocumentActions Tombstones();
        IDocumentActions Conflicts();
        IIndexActions Indexes();
        IKeyValueActions<long> Identities();
        IKeyValueActions<BlittableJsonReaderObject> CompareExchange(JsonOperationContext context);
        ICounterActions Counters();
    }

    public interface IDocumentActions : INewDocumentActions, IDisposable
    {
        void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress);
        void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress);
        void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress);
        void DeleteDocument(string id);
    }

    public interface INewDocumentActions
    {
        DocumentsOperationContext GetContextForNewDocument();
        Stream GetTempStream();
    }

    public interface IIndexActions : IDisposable
    {
        void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType);
        void WriteIndex(IndexDefinition indexDefinition);
    }

    public interface ICounterActions : IDisposable
    {
        void WriteCounter(CounterDetail counterDetail);
    }

    public interface IKeyValueActions<in T> : IDisposable
    {
        void WriteKeyValue(string key, T value);
    }

    public interface IDatabaseRecordActions : IDisposable
    {
        void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType);
    }
}
