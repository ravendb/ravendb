using System;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerDestination
    {
        IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion);
        IDocumentActions Documents();
        IDocumentActions RevisionDocuments();
        IIndexActions Indexes();
        IIdentityActions Identities();
    }

    public interface IDocumentActions : INewDocumentActions, IDisposable
    {
        void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress);
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

    public interface IIdentityActions : IDisposable
    {
        void WriteIdentity(string key, long value);
    }
}
