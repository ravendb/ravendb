using System;
using Raven.Database.Storage.StorageActions;

namespace Raven.Database.Storage
{
    public interface IStorageActionsAccessor
    {
        ITransactionStorageActions Transactions { get; }
        IDocumentStorageActions Documents { get; }
        IQueueStorageActions Queue { get; }
        ITasksStorageActions Tasks { get; }
        IStalenessStorageActions Staleness{ get; }
        IAttachmentsStorageActions Attachments { get; }
        IIndexingStorageActions Indexing { get; }
        IGeneralStorageActions General { get; }
        IMappedResultsStorageAction MappedResults { get; }
        event Action OnCommit;
    }
}