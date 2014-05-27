using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client.Changes
{
    public interface IDatabaseChanges : IConnectableChanges<IDatabaseChanges>
	{
		IObservableWithTask<IndexChangeNotification> ForIndex(string indexName);
		IObservableWithTask<DocumentChangeNotification> ForDocument(string docId);
		IObservableWithTask<DocumentChangeNotification> ForAllDocuments();
		IObservableWithTask<IndexChangeNotification> ForAllIndexes();
	    IObservableWithTask<TransformerChangeNotification> ForAllTransformers();
        IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix);
		IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection(string collectionName);
		IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection<TEntity>();
		IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(string typeName);
		IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(Type type);
		IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType<TEntity>();
		IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts();
		IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid operationId);
	}
}