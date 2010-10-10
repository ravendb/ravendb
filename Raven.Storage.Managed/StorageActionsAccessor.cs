using System;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class StorageActionsAccessor : IStorageActionsAccessor
    {
        private readonly TableStorage storage;

        public StorageActionsAccessor(TableStorage storage)
        {
            this.storage = storage;
            General = new GeneralStorageActions(storage);
            Attachments = new AttachmentsStorageActions(storage);
            Transactions = new TransactionStorageActions(storage);
            Documents = new DocumentsStorageActions(storage, Transactions);
            Indexing = new IndexingStorageActions(storage);
        }

        public ITransactionStorageActions Transactions
        {
            get; private set;
        }

        public IDocumentStorageActions Documents
        {
            get; private set;
        }

        public IQueueStorageActions Queue
        {
            get { throw new NotImplementedException(); }
        }

        public ITasksStorageActions Tasks
        {
            get { throw new NotImplementedException(); }
        }

        public IAttachmentsStorageActions Attachments
        {
            get; private set;
        }

        public IIndexingStorageActions Indexing
        {
            get;
            private set;
        }

        public IGeneralStorageActions General
        {
            get; private set;
        }

        public IMappedResultsStorageAction MappedResults
        {
            get { throw new NotImplementedException(); }
        }

        public event Action OnCommit;
    }
}