using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;

namespace Raven.Client.Embedded.Changes
{
	internal class EmbeddableDatabaseChanges : IDatabaseChanges, IDisposable
	{
		private readonly Action onDispose;
		private readonly EmbeddableObserableWithTask<IndexChangeNotification> indexesObservable;
		private readonly EmbeddableObserableWithTask<DocumentChangeNotification> documentsObservable;

		public EmbeddableDatabaseChanges(EmbeddableDocumentStore embeddableDocumentStore, Action onDispose)
		{
			this.onDispose = onDispose;
			Task = new CompletedTask();
			indexesObservable = new EmbeddableObserableWithTask<IndexChangeNotification>();
			documentsObservable = new EmbeddableObserableWithTask<DocumentChangeNotification>();

			embeddableDocumentStore.DocumentDatabase.TransportState.OnIndexChangeNotification += indexesObservable.Notify;
			embeddableDocumentStore.DocumentDatabase.TransportState.OnDocumentChangeNotification += documentsObservable.Notify;
		}

		public Task Task { get; private set; }

		public IObservableWithTask<IndexChangeNotification> ForIndex(string indexName)
		{
			return new FilteringObservableWithTask<IndexChangeNotification>(indexesObservable,
				notification => string.Equals(indexName, notification.Name, StringComparison.InvariantCultureIgnoreCase));
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
				notification => string.Equals(docId, notification.Name, StringComparison.InvariantCultureIgnoreCase));
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
		{
			if (docIdPrefix == null) throw new ArgumentNullException("docIdPrefix");

			return new FilteringObservableWithTask<DocumentChangeNotification>(documentsObservable,
				notification => notification.Name.StartsWith(docIdPrefix, StringComparison.InvariantCultureIgnoreCase));
		}

		public void Dispose()
		{
			onDispose();
		}
	}
}