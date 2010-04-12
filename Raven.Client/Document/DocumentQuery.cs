using System;
using System.Threading;
using Raven.Client.Client;
using Raven.Database.Data;

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
				var result = databaseCommands.Query(indexName, query, start, pageSize);
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