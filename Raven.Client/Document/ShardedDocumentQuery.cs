using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using System.Linq;

namespace Raven.Client.Document
{
	public class ShardedDocumentQuery<T> : IDocumentQuery<T>
	{
		private readonly IDocumentQuery<T>[] queries;
		private QueryResult queryResult;

		public ShardedDocumentQuery(string indexName, IList<IDocumentSession> shardSessions)
		{
			queries = new IDocumentQuery<T>[shardSessions.Count];
			for (int i = 0; i < shardSessions.Count; i++)
			{
				queries[i] = shardSessions[i].Query<T>(indexName);
			}
		}

	    private ShardedDocumentQuery(IDocumentQuery<T>[] queries)
	    {
	        this.queries = queries;
	    }

	    protected QueryResult GetQueryResult()
		{
			var queryResults = queries.Select(x => x.QueryResult).ToArray();
			return new QueryResult
			{
				IsStale = queryResults.Any(x => x.IsStale),
				Results = queryResults.SelectMany(x => x.Results).ToArray(),
				TotalResults = queryResults.Sum(x => x.TotalResults)
			};
		}

		private void ApplyForAll(Action<IDocumentQuery<T>> act)
		{
			foreach (var query in queries)
			{
				act(query);
			}
		}

		public IEnumerator<T> GetEnumerator()
		{
			var jsonSerializer = new JsonSerializer();
			return QueryResult.Results
				.Select(j => (T)jsonSerializer.Deserialize(new JsonTokenReader(j), typeof(T)))
				.GetEnumerator();
		}


		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public IDocumentQuery<T> Take(int count)
		{
			ApplyForAll(query => query.Take(count));
			return this;
		}

		public IDocumentQuery<T> Skip(int count)
		{
			ApplyForAll(query => query.Skip(count));
			return this;
		}

		public IDocumentQuery<T> Where(string whereClause)
		{
			ApplyForAll(query => query.Where(whereClause));
			return this;
		}

		public IDocumentQuery<T> OrderBy(params string[] fields)
		{
			ApplyForAll(query => query.OrderBy(fields));
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResults()
		{
			ApplyForAll(query => query.WaitForNonStaleResults());
			return this;
		}

	    public IDocumentQuery<TProjection> Select<TProjection>(Func<T, TProjection> projectionExpression)
	    {
	        return new ShardedDocumentQuery<TProjection>(
	            queries.Select(x => x.Select(projectionExpression)).ToArray()
	            );
	    }

	    public QueryResult QueryResult
		{
			get { return queryResult ?? (queryResult = GetQueryResult()); }
		}
	}
}