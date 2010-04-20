using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using Raven.Client.Client;
using Raven.Database.Data;
using System.Linq;

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
				timeout = timeout,
	            waitForNonStaleResults = waitForNonStaleResults
	        };
	    }

	    protected override QueryResult GetQueryResult()
		{
	    	var sp = Stopwatch.StartNew();
			while (true) 
			{
				var result = databaseCommands.Query(indexName, new IndexQuery
				{
					Query = query,
					PageSize = pageSize,
					Start = start,
					SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray()
				});
				if(waitForNonStaleResults && result.IsStale)
				{
					if (sp.Elapsed > timeout)
					{
						sp.Stop();
						throw new TimeoutException(string.Format("Waited for {0:#,#}ms for the query to return non stale result.", sp.ElapsedMilliseconds));
					}
					Thread.Sleep(100);
					continue;
				}
				return result;
			} 
		}
	}
}