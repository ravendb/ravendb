// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerEmbeddedDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
	public class DatabaseSmugglerEmbeddedDestination : IDatabaseSmugglerDestination
	{
		private readonly DocumentDatabase _database;

		public DatabaseSmugglerEmbeddedDestination(DocumentDatabase database)
		{
			_database = database;
		}

		public void Dispose()
		{
			throw new System.NotImplementedException();
		}

		public bool SupportsOperationState { get; }

		public bool SupportsWaitingForIndexing { get; }

		public Task InitializeAsync(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerIndexActions IndexActions()
		{
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerDocumentActions DocumentActions()
		{
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerTransformerActions TransformerActions()
		{
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions()
		{
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			throw new System.NotImplementedException();
		}

		public Task<DatabaseSmugglerOperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task SaveOperationStateAsync(DatabaseSmugglerOptions options, DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task WaitForIndexingAsOfLastWriteAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}
	}
}