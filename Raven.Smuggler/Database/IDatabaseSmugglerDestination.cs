using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDestination : IDisposable
	{
		Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken);

		IDatabaseSmugglerIndexActions IndexActions();

		IDatabaseSmugglerDocumentActions DocumentActions();

		IDatabaseSmugglerTransformerActions TransformerActions();

		IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions();

		IDatabaseSmugglerIdentityActions IdentityActions();

		OperationState ModifyOperationState(DatabaseSmugglerOptions options, OperationState state);
	}
}