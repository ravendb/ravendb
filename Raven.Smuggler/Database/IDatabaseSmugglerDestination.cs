using System;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDestination : IDisposable
	{
		void Initialize(DatabaseSmugglerOptions options);

		IDatabaseSmugglerIndexActions IndexActions();

		IDatabaseSmugglerDocumentActions DocumentActions();

		IDatabaseSmugglerTransformerActions TransformerActions();

		IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions();

		IDatabaseSmugglerIdentityActions IdentityActions();

		OperationState ModifyOperationState(DatabaseSmugglerOptions options, OperationState state);
	}
}