using System;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class StorageActionsAccessor : IStorageActionsAccessor
    {
        public StorageActionsAccessor(TableStorage storage)
        {
            General = new GeneralStorageActions(storage);
            Attachments = new AttachmentsStorageActions(storage);
            Transactions = new TransactionStorageActions(storage);
            Documents = new DocumentsStorageActions(storage, Transactions);
            Indexing = new IndexingStorageActions(storage);
            MappedResults = new MappedResultsStorageAction(storage);
            Queue = new QueueStorageActions(storage);
            Tasks = new TasksStorageActions(storage);
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
            get; private set;
        }

        public ITasksStorageActions Tasks
        {
            get; private set;
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
        { get; private set;
        }

        public event Action OnCommit;
    }
}