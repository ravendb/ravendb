using System;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDestination : IDisposable
	{
		void Initialize();

		IDatabaseSmugglerIndexActions IndexActions();

		IDatabaseSmugglerDocumentActions DocumentActions();

		IDatabaseSmugglerTransformerActions TransformerActions();

		IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions();

		IDatabaseSmugglerIdentityActions IdentityActions();
	}
}