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

		Task InitializeAsync(DatabaseSmugglerOptions options, Report report, CancellationToken cancellationToken);

		IDatabaseSmugglerIndexActions IndexActions();

		IDatabaseSmugglerDocumentActions DocumentActions();

		IDatabaseSmugglerTransformerActions TransformerActions();

		IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions();

		IDatabaseSmugglerIdentityActions IdentityActions();

		Task<OperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options);

		Task SaveOperationStateAsync(DatabaseSmugglerOptions options, OperationState state);
	}
}