using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Database.Plugins.Builtins
{
	public class CreateSilverlightIndexes : ISilverlightRequestedAware
	{
		public void SilverlightWasRequested(DocumentDatabase database)
		{
			var ravenDocumentsByEntityName = new RavenDocumentsByEntityName {};
			database.Indexes.PutIndex(Constants.DocumentsByEntityNameIndex,
				ravenDocumentsByEntityName.CreateIndexDefinition());
		}
	}
}