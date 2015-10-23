// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerEmbeddedDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Util;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
	public class DatabaseSmugglerEmbeddedDestination : IDatabaseSmugglerDestination
	{
		private readonly DocumentDatabase _database;

		private DatabaseSmugglerOptions _globalOptions;

		public DatabaseSmugglerEmbeddedDestination(DocumentDatabase database)
		{
			_database = database;
		}

		public void Dispose()
		{
		}

		public bool SupportsOperationState => false;

		public bool SupportsWaitingForIndexing => false;

		public Task InitializeAsync(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, CancellationToken cancellationToken)
		{
			_globalOptions = options;
			return new CompletedTask();
		}

		public IDatabaseSmugglerIndexActions IndexActions()
		{
			return new DatabaseSmugglerEmbeddedIndexActions(_database);
		}

		public IDatabaseSmugglerDocumentActions DocumentActions()
		{
			return new DatabaseSmugglerEmbeddedDocumentActions(_globalOptions, _database);
		}

		public IDatabaseSmugglerTransformerActions TransformerActions()
		{
			return new DatabaseSmugglerEmbeddedTransformerActions(_database);
		}

		public IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions()
		{
			return new DatabaseSmugglerEmbeddedDocumentDeletionActions(_database);
		}

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			return new DatabaseSmugglerEmbeddedIdentityActions(_database);
		}

		public Task<DatabaseSmugglerOperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SaveOperationStateAsync(DatabaseSmugglerOptions options, DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task WaitForIndexingAsOfLastWriteAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}
	}
}