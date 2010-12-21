//-----------------------------------------------------------------------
// <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class StorageActionsAccessor : IStorageActionsAccessor
    {
        private readonly IEnumerable<AbstractDocumentCodec> documentCodecs;

        public StorageActionsAccessor(TableStorage storage, IUuidGenerator generator, IEnumerable<AbstractDocumentCodec> documentCodecs)
        {
            this.documentCodecs = documentCodecs;
            General = new GeneralStorageActions(storage);
            Attachments = new AttachmentsStorageActions(storage, generator);
            Transactions = new TransactionStorageActions(storage, generator, documentCodecs);
            Documents = new DocumentsStorageActions(storage, Transactions, generator, documentCodecs);
            Indexing = new IndexingStorageActions(storage);
            MappedResults = new MappedResultsStorageAction(storage, generator);
            Queue = new QueueStorageActions(storage, generator);
            Tasks = new TasksStorageActions(storage, generator);
            Staleness = new StalenessStorageActions(storage);
        }


        public ITransactionStorageActions Transactions
        {
            get;
            private set;
        }

        public IDocumentStorageActions Documents
        {
            get;
            private set;
        }

        public IQueueStorageActions Queue
        {
            get;
            private set;
        }

        public ITasksStorageActions Tasks
        {
            get;
            private set;
        }

        public IStalenessStorageActions Staleness
        {
            get;
            private set;
        }

        public IAttachmentsStorageActions Attachments
        {
            get;
            private set;
        }

        public IIndexingStorageActions Indexing
        {
            get;
            private set;
        }

        public IGeneralStorageActions General
        {
            get;
            private set;
        }

        public IMappedResultsStorageAction MappedResults
        {
            get;
            private set;
        }

        public event Action OnCommit;

        [DebuggerNonUserCode]
        public void InvokeOnCommit()
        {
            var handler = OnCommit;
            if (handler != null)
                handler();
        }
    }
}