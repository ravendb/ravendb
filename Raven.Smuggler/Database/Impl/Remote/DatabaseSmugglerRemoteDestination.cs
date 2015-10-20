// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerRemoteDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Client;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteDestination : IDatabaseSmugglerDestination
	{
		private readonly IDocumentStore _store;

		public DatabaseSmugglerRemoteDestination(IDocumentStore store)
		{
			_store = store;
		}

		public void Dispose()
		{
		}

		public Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public IDatabaseSmugglerIndexActions IndexActions()
		{
			return new DatabaseSmugglerRemoteIndexActions(_store);
		}

		public IDatabaseSmugglerDocumentActions DocumentActions()
		{
			return new DatabaseSmugglerRemoteDocumentActions(_store);
		}

		public IDatabaseSmugglerTransformerActions TransformerActions()
		{
			return new DatabaseSmugglerRemoteTransformerActions(_store);
		}

		public IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions()
		{
			return new DatabaseSmugglerRemoteDocumentDeletionActions(_store);
		}

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			return new DatabaseSmugglerRemoteIdentityActions(_store);
		}

		public OperationState ModifyOperationState(DatabaseSmugglerOptions options, OperationState state)
		{
			return state;
		}
	}
}