using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	public class ShardedDocumentQuery<T> : IDocumentQuery<T>
	{
		private readonly IList<IDocumentSession> shardSessions;
		private readonly IDocumentQuery<T>[] queries;
		private QueryResult queryResult;

		public ShardedDocumentQuery(string indexName, IList<IDocumentSession> shardSessions)
		{
			this.shardSessions = shardSessions;
			queries = new IDocumentQuery<T>[shardSessions.Count];
			for (int i = 0; i < shardSessions.Count; i++)
			{
				queries[i] = shardSessions[i].LuceneQuery<T>(indexName);
			}
		}

		private ShardedDocumentQuery(IDocumentQuery<T>[] queries, IList<IDocumentSession> shardSessions)
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
			var jsonSerializer = new JsonSerializer
			{
                // we assume the same json contract resolver across the entire shared sessions set
				ContractResolver = shardSessions.First().Conventions.JsonContractResolver,
				ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
			};
			return QueryResult.Results
				.Select(j => (T)jsonSerializer.Deserialize(new JTokenReader(j), typeof(T)))
				.GetEnumerator();
		}

		public void ForEachQuery(Action<IDocumentSession,IDocumentQuery<T>> action)
		{
			for (int i = 0; i < shardSessions.Count; i++)
			{
				action(shardSessions[i], queries[i]);
			}
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

		public IDocumentQuery<T> WhereEquals(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereEquals(fieldName, value));
			return this;
		}

		public IDocumentQuery<T> WhereContains(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereContains(fieldName, value));
			return this;
		}

		public IDocumentQuery<T> And()
		{
			ApplyForAll(query => query.And());
			return this;
		}

		public IDocumentQuery<T> Or()
		{
			ApplyForAll(query => query.Or());
			return this;
		}

		public IDocumentQuery<T> Boost(decimal boost)
		{
			ApplyForAll(query => query.Boost(boost));
			return this;
		}

		public IDocumentQuery<T> Fuzzy(decimal fuzzy)
		{
			ApplyForAll(query => query.Fuzzy(fuzzy));
			return this;
		}

		public IDocumentQuery<T> Proximity(int proximity)
		{
			ApplyForAll(query => query.Proximity(proximity));
			return this;
		}

		public IDocumentQuery<T> OrderBy(params string[] fields)
		{
			ApplyForAll(query => query.OrderBy(fields));
			return this;
		}

	    public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow()
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOfNow());
            return this;
	    }

	    public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOfNow(waitTimeout));
            return this;
	    }

	    public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff)
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOf(cutOff));
            return this;
	    }

	    public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOf(cutOff, waitTimeout));
            return this;
	    }

	    public IDocumentQuery<T> WaitForNonStaleResults()
		{
			ApplyForAll(query => query.WaitForNonStaleResults());
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResults(TimeSpan timeout)
		{
			ApplyForAll(query => query.WaitForNonStaleResults(timeout));
			return this;
		}

		public IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fieds)
	    {
	        return new ShardedDocumentQuery<TProjection>(
				queries.Select(x => x.SelectFields<TProjection>(fieds)).ToArray(), shardSessions
	            );
	    }

	    public QueryResult QueryResult
		{
			get { return queryResult ?? (queryResult = GetQueryResult()); }
		}
	}
}