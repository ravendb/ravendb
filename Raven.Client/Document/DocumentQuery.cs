using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Raven.Client.Client;
using Raven.Database.Data;
using System.Linq;
using Raven.Database.Indexing;

namespace Raven.Client.Document
{
	public class DocumentQuery<T> : AbstractDocumentQuery<T>
	{
		private readonly IDatabaseCommands databaseCommands;

	    public DocumentQuery(IDatabaseCommands databaseCommands, string indexName, string[] projectionFields)
		{
			this.databaseCommands = databaseCommands;
		    this.projectionFields = projectionFields;
		    this.indexName = indexName;
		}

	    public override IDocumentQuery<TProjection> Select<TProjection>(Func<T, TProjection> projectionExpression)
	    {
	        return new DocumentQuery<TProjection>(databaseCommands, indexName,
	                                              projectionExpression
	                                                  .Method
	                                                  .ReturnType
	                                                  .GetProperties(BindingFlags.Instance | BindingFlags.Public)
	                                                  .Select(x => x.Name).ToArray()
	            )
	        {
	            pageSize = pageSize,
	            query = query,
	            start = start,
	            waitForNonStaleResults = waitForNonStaleResults
	        };
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