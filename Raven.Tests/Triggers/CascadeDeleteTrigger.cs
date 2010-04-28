using System;
using System.ComponentModel.Composition;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Tests.Triggers
{
	[Export(typeof(IDeleteTrigger))]
	public class CascadeDeleteTrigger : IDeleteTrigger, IRequiresDocumentDatabaseInitialization 
	{
		private DocumentDatabase docDb;
		public VetoResult AllowDelete(string key)
		{
			return VetoResult.Allowed;
		}

		public void OnDelete(string key)
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