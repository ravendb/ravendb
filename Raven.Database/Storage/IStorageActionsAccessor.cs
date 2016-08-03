//-----------------------------------------------------------------------
// <copyright file="IStorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Impl.DTC;

namespace Raven.Database.Storage
{
    public interface IStorageActionsAccessor : IDisposable
    {
        IDocumentStorageActions Documents { get; }
        IQueueStorageActions Queue { get; }
        IListsStorageActions Lists { get; }
        ITasksStorageActions Tasks { get; }
        IStalenessStorageActions Staleness { get; }
        
        [Obsolete("Use RavenFS instead.")]
        IAttachmentsStorageActions Attachments { get; }
        
        IIndexingStorageActions Indexing { get; }
        IGeneralStorageActions General { get; }
        IMappedResultsStorageAction MapReduce { get; }
        IInFlightStateSnapshot InFlightStateSnapshot { get; }
        bool IsNested { get; set; }
        event Action OnStorageCommit;
        event Action BeforeStorageCommit;
        event Action AfterStorageCommit;
        T GetTask<T>(Func<T, bool> predicate, T newTask) where T : Tasks.DatabaseTask;
        void AfterStorageCommitBeforeWorkNotifications(JsonDocument doc, Action<JsonDocument[]> afterCommit);
    }
}
