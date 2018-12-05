//-----------------------------------------------------------------------
// <copyright file="ITransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface ITransactionalStorage : IDisposable
	{
		/// <summary>
		/// This is used mostly for replication
		/// </summary>
		Guid Id { get; }

		IDisposable DisableBatchNesting();

		void Batch(Action<IStorageActionsAccessor> action);
		void ExecuteImmediatelyOrRegisterForSynchronization(Action action);
		bool Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs);
		void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument documentDatabase);
		void Restore(string backupLocation, string databaseLocation, Action<string> output, bool defrag);
		long GetDatabaseSizeInBytes();
		long GetDatabaseCacheSizeInBytes();
		long GetDatabaseTransactionVersionSizeInBytes();

		string FriendlyName { get; }
		bool HandleException(Exception exception);

		bool IsAlreadyInBatch { get; }

		void Compact(InMemoryRavenConfiguration configuration);
		Guid ChangeId();
		void ClearCaches();
		void DumpAllStorageTables();
		InFlightTransactionalState GetInFlightTransactionalState(DocumentDatabase self, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete);
        IList<string> ComputeDetailedStorageInformation();
        List<TransactionContextData> GetPreparedTransactions();

		object GetInFlightTransactionsInternalStateForDebugOnly();
	    void DropAllIndexingInformation();
	}
}
