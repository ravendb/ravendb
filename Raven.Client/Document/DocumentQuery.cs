using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using Raven.Client.Client;
using Raven.Database.Data;
using System.Linq;
using System.Text;

namespace Raven.Client.Document
{
	public class DocumentQuery<T> : AbstractDocumentQuery<T>
	{
		private readonly IDatabaseCommands databaseCommands;

	    public DocumentQuery(DocumentSession session, IDatabaseCommands databaseCommands, string indexName, string[] projectionFields):base(session)
		{
	    	this.databaseCommands = databaseCommands;
		    this.projectionFields = projectionFields;
		    this.indexName = indexName;
		}

		public override IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields)
	    {
			return new DocumentQuery<TProjection>(session, databaseCommands, indexName,fields)
	        {
	            pageSize = pageSize,
	            queryText = new StringBuilder(queryText.ToString()),
	            start = start,
				timeout = timeout,
                cutoff = cutoff,
	            waitForNonStaleResults = waitForNonStaleResults
	        };
	    }

	    protected override QueryResult GetQueryResult()
		{
	        session.IncrementRequestCount();
            var sp = Stopwatch.StartNew();
			while (true) 
			{
				string query = queryText.ToString();

				Trace.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
								query, indexName, session.StoreIdentifier));
				var result = databaseCommands.Query(indexName, new IndexQuery
				{
					Query = query,
					PageSize = pageSize,
					Start = start,
                    Cutoff = cutoff,
					SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
					FieldsToFetch = projectionFields
				});
				if(waitForNonStaleResults && result.IsStale)
				{
					if (sp.Elapsed > timeout)
					{
						sp.Stop();
						throw new TimeoutException(string.Format("Waited for {0:#,#}ms for the query to return non stale result.", sp.ElapsedMilliseconds));
					}
					Trace.WriteLine(
						string.Format("Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retired",
						              query, indexName, session.StoreIdentifier));
					Thread.Sleep(100);
					continue;
				}
				Trace.WriteLine(string.Format("Query returned {0}/{1} results", result.Results.Length, result.TotalResults));
				return result;
			} 
		}
	}
}