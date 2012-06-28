//-----------------------------------------------------------------------
// <copyright file="ITransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Impl;

namespace Raven.Database.Storage
{
	public interface ITransactionalStorage : IDisposable
	{
		/// <summary>
		/// This is used mostly for replication
		/// </summary>
		Guid Id { get; }
		void Batch(Action<IStorageActionsAccessor> action);
		void ExecuteImmediatelyOrRegisterForSyncronization(Action action);
		bool Initialize(IUuidGenerator generator);
		void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup);
		void Restore(string backupLocation, string databaseLocation);
		long GetDatabaseSizeInBytes();

		string FriendlyName { get; }
		bool HandleException(Exception exception);
	}
}
