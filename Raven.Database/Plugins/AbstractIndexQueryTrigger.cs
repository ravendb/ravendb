using System;
using System.ComponentModel.Composition;
using Lucene.Net.Search;
using Raven.Abstractions.Data;

namespace Raven.Database.Plugins
{
	/// <summary>
	/// Allows to extend the query parsing capability of RavenDB, providing users with a way to modify
	/// the queries before they are executed against the index
	/// </summary>
	[InheritedExport]
	public abstract class AbstractIndexQueryTrigger : IRequiresDocumentDatabaseInitialization
	{
		public void Initialize(DocumentDatabase database)
		{
			Database = database;
			Initialize();
		}

		public virtual void Initialize()
		{
			
		}
		public virtual void SecondStageInit()
		{

		}


		public DocumentDatabase Database { get; set; }

		public abstract Query ProcessQuery(string indexName, Query query, IndexQuery originalQuery);
	}
}