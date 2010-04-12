using System;
using System.Threading;
using Raven.Client.Client;
using Raven.Database.Data;
using Raven.Database.Indexing;

namespace Raven.Client.Document
{
	public class DocumentQuery<T> : AbstractDocumentQuery<T>
	{
		private readonly IDatabaseCommands databaseCommands;

		public DocumentQuery(IDatabaseCommands databaseCommands, string indexName)
		{
			this.databaseCommands = databaseCommands;
			this.indexName = indexName;
		}

		protected override QueryResult GetQueryResult()
		{
			while (true) 
			{
				var result = databaseCommands.Query(indexName, new IndexQuery(query, start, pageSize));
				if(waitForNonStaleResults && result.IsStale)
				{
					Thread.Sleep(100);
					continue;
				}
				return result;
			} 
		}
	}
}