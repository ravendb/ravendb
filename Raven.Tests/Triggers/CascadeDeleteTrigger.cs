using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Tests.Triggers
{
	public class CascadeDeleteTrigger : IDeleteTrigger, IRequiresDocumentDatabaseInitialization 
	{
		private DocumentDatabase docDb;
        public VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		public void OnDelete(string key, TransactionInformation transactionInformation)
		{
			var document = docDb.Get(key, null);
			if (document == null)
				return;
			docDb.Delete(document.Metadata.Value<string>("Cascade-Delete"), null, null);
		}

		public void AfterCommit(string key)
		{
		}

		public void Initialize(DocumentDatabase database)
		{
			docDb = database;
		}
	}
}