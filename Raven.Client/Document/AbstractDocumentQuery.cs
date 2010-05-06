using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	public abstract class AbstractDocumentQuery<T> : IDocumentQuery<T>
	{
		protected readonly DocumentSession session;
		protected string indexName;
		protected string query = "";
		protected string[] orderByFields = new string[0];
		protected int start;
		protected int pageSize = 128;
		protected bool waitForNonStaleResults;
		protected TimeSpan timeout;
        protected string[] projectionFields;
        private QueryResult queryResult;

		protected AbstractDocumentQuery(DocumentSession session)
		{
			this.session = session;
		}

		public IDocumentQuery<T> WaitForNonStaleResults(TimeSpan waitTimeout)
		{
			waitForNonStaleResults = true;
			timeout = waitTimeout;
			return this;
		}

		public abstract IDocumentQuery<TProjection> Select<TProjection>(Func<T, TProjection> projectionExpression);

	    public QueryResult QueryResult
		{
			get { return queryResult ?? (queryResult = GetQueryResult()); }
		}

		public IEnumerator<T> GetEnumerator()
		{
			return QueryResult.Results
				.Select(Deserialize)
				.GetEnumerator();
		}

		private T Deserialize(JObject result)
		{
			var metadata = result.Value<JObject>("@metadata");
			if (projectionFields != null && projectionFields.Length > 0  // we asked for a projection directly from the index
				|| metadata == null)									 // we aren't querying a document, we are probably querying a map reduce index result
				return (T)new JsonSerializer().Deserialize(new JsonTokenReader(result), typeof(T));
			return session.TrackEntity<T>(metadata.Value<string>("@id"),
			                              result,
			                              metadata);
		}

		protected abstract QueryResult GetQueryResult();

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public IDocumentQuery<T> Take(int count)
		{
			pageSize = count;
			return this;
		}

		public IDocumentQuery<T> Skip(int count)
		{
			start = count;
			return this;
		}

		public IDocumentQuery<T> Where(string whereClause)
		{
			if (string.IsNullOrEmpty(query))
				query = whereClause;
			else
				query += " " + whereClause;
			return this;
		}

		public IDocumentQuery<T> OrderBy(params string[] fields)
		{
			orderByFields = orderByFields.Concat(fields).ToArray();
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResults()
		{
			waitForNonStaleResults = true;
			timeout = TimeSpan.FromSeconds(15);
			return this;
		}
	}
}