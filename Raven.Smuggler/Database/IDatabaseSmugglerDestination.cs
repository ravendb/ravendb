using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDestination : IDisposable
	{
		bool SupportsOperationState { get; }

		bool SupportsWaitingForIndexing { get; }

		Task InitializeAsync(DatabaseSmugglerOptions options, Report report, CancellationToken cancellationToken);

		IDatabaseSmugglerIndexActions IndexActions();

		IDatabaseSmugglerDocumentActions DocumentActions();

		IDatabaseSmugglerTransformerActions TransformerActions();

		IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions();

		IDatabaseSmugglerIdentityActions IdentityActions();

		Task<OperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken);

		Task SaveOperationStateAsync(DatabaseSmugglerOptions options, OperationState state, CancellationToken cancellationToken);

		Task WaitForIndexingAsOfLastWriteAsync(CancellationToken cancellationToken);
	}
}