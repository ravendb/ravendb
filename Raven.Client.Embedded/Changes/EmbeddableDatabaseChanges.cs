using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Changes;

namespace Raven.Client.Embedded.Changes
{
	using Raven.Client.Document;

	internal class EmbeddableDatabaseChanges : IDatabaseChanges, IDisposable
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private readonly Action onDispose;
		private readonly Func<string, Etag, string[], bool> tryResolveConflictByUsingRegisteredConflictListeners;
		private readonly EmbeddableObservableWithTask<IndexChangeNotification> indexesObservable;
		private readonly EmbeddableObservableWithTask<DocumentChangeNotification> documentsObservable;
		private readonly EmbeddableObservableWithTask<BulkInsertChangeNotification> bulkInsertObservable;
		private readonly EmbeddableObservableWithTask<ReplicationConflictNotification> replicationConflictsObservable;

		private readonly BlockingCollection<Action> enqueuedActions = new BlockingCollection<Action>();
		private readonly Task enqueuedTask;

		private readonly DocumentConvention conventions;

		public EmbeddableDatabaseChanges(EmbeddableDocumentStore embeddableDocumentStore, Action onDispose, Func<string, Etag, string[], bool> tryResolveConflictByUsingRegisteredConflictListeners)
		{
			this.onDispose = onDispose;
			this.tryResolveConflictByUsingRegisteredConflictListeners = tryResolveConflictByUsingRegisteredConflictListeners;
			conventions = embeddableDocumentStore.Conventions;
			Task = new CompletedTask<IDatabaseChanges>(this);
			indexesObservable = new EmbeddableObservableWithTask<IndexChangeNotification>();
			documentsObservable = new EmbeddableObservableWithTask<DocumentChangeNotification>();
			bulkInsertObservable = new EmbeddableObservableWithTask<BulkInsertChangeNotification>();
			replicationConflictsObservable = new EmbeddableObservableWithTask<ReplicationConflictNotification>();

			embeddableDocumentStore.DocumentDatabase.TransportState.OnIndexChangeNotification += (o, notification) =>
				enqueuedActions.Add(() => indexesObservable.Notify(o, notification));
			embeddableDocumentStore.DocumentDatabase.TransportState.OnDocumentChangeNotification += (o, notification) =>
				 enqueuedActions.Add(() => documentsObservable.Notify(o, notification));
			embeddableDocumentStore.DocumentDatabase.TransportState.OnReplicationConflictNotification += (o, notification) =>
				 enqueuedActions.Add(() => replicationConflictsObservable.Notify(o, notification));
			embeddableDocumentStore.DocumentDatabase.TransportState.OnReplicationConflictNotification += TryResolveConflict;
			embeddableDocumentStore.DocumentDatabase.TransportState.OnBulkInsertChangeNotification += (o, notification) =>
			                                                                                          enqueuedActions.Add(() =>
			                                                                                          {
				                                                                                          bulkInsertObservable.Notify(o, notification);
																										  documentsObservable.Notify(o, notification);
			                                                                                          });

			enqueuedTask = System.Threading.Tasks.Task.Factory.StartNew(() =>
			{
				while (true)
				{
					var action = enqueuedActions.Take();
					if (action == null)
						return;
					action();
				}
			});
		}

		void TryResolveConflict(object obj, ReplicationConflictNotification conflictNotification)
		{
			if (conflictNotification.ItemType == ReplicationConflictTypes.DocumentReplicationConflict)
			{
				System.Threading.Tasks.Task.Factory.StartNew(() =>
					  tryResolveConflictByUsingRegisteredConflictListeners(conflictNotification.Id, conflictNotification.Etag, conflictNotification.Conflicts))
						.ContinueWith(t =>
						{
							t.AssertNotFailed();

							if (t.Result)
							{
								logger.Debug(
									"Document replication conflict for {0} was resolved by one of the registered conflict listeners",
									conflictNotification.Id);
							}
						});
			}
		}

		public bool Connected { get; private set; }
		public event EventHandler ConnectionStatusChanged = delegate { };
		public Task<IDatabaseChanges> Task { get; private set; }

		public IObservableWithTask<IndexChangeNotification> ForIndex(string indexName)
		{
			return new FilteringObservableWithTask<IndexChangeNotification>(indexesObservable,
				notification => string.Equals(indexName, notification.Name, StringComparison.OrdinalIgnoreCase));
		}

		public IObservableWithTask<DocumentChangeNotification> ForAllDocuments()
		{
			return new FilteringObservableWithTask<DocumentChangeNotification>(documentsObservable,
				notification => true);
		}

		public IObservableWithTask<IndexChangeNotification> ForAllIndexes()
		{
			return new FilteringObservableWithTask<IndexChangeNotification>(indexesObservable,
				notification => true);
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocument(string docId)
		{
			return new FilteringObservableWithTask<DocumentChangeNotification>(documentsObservable,
				notification => string.Equals(docId, notification.Id, StringComparison.OrdinalIgnoreCase));
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
		{
			if (docIdPrefix == null) throw new ArgumentNullException("docIdPrefix");

			return new FilteringObservableWithTask<DocumentChangeNotification>(documentsObservable,
				notification => notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection(string collectionName)
		{
			if (collectionName == null) throw new ArgumentNullException("collectionName");

			return new FilteringObservableWithTask<DocumentChangeNotification>(documentsObservable,
				notification => string.Equals(collectionName, notification.CollectionName, StringComparison.OrdinalIgnoreCase));
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection<TEntity>()
		{
			var collectionName = conventions.GetTypeTagName(typeof(TEntity));
			return ForDocumentsInCollection(collectionName);
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(string typeName)
		{
			if (typeName == null) throw new ArgumentNullException("typeName");

			return new FilteringObservableWithTask<DocumentChangeNotification>(documentsObservable,
				notification => string.Equals(typeName, notification.TypeName, StringComparison.OrdinalIgnoreCase));
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(Type type)
		{
			if (type == null) throw new ArgumentNullException("type");

			var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(type);
			return ForDocumentsOfType(typeName);
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType<TEntity>()
		{
			var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(TEntity));
			return ForDocumentsOfType(typeName);
		}

		public IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts()
		{
			return new FilteringObservableWithTask<ReplicationConflictNotification>(replicationConflictsObservable,
																					notification => true);
		}

		public IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid operationId)
		{
			return new FilteringObservableWithTask<BulkInsertChangeNotification>(bulkInsertObservable,
				notification => notification.OperationId == operationId);
		}

		public void WaitForAllPendingSubscriptions()
		{
			// nothing there to do
		}

		public void Dispose()
		{
			enqueuedActions.Add(null);
			onDispose();
			enqueuedTask.Wait();
		}
	}
}