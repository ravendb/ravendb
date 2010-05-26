using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;

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
	    protected DateTime? cutoff;

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

		public abstract IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields);

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
			{
				return (T)new JsonSerializer
				{
                    ContractResolver = session.Conventions.JsonContractResolver,
					ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
					Converters =
						{
							new JsonEnumConverter(),
						}
				}.Deserialize(new JTokenReader(result), typeof(T));
			}
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

	    public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow()
	    {
	        waitForNonStaleResults = true;
	        cutoff = DateTime.Now;
	        return this;
	    }

        public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
	    {
            waitForNonStaleResults = true;
            cutoff = DateTime.Now;
            timeout = waitTimeout;
            return this;
	    }

	    public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff)
	    {
            waitForNonStaleResults = true;
            this.cutoff = cutOff;
            return this;
	    }

        public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
	    {
            waitForNonStaleResults = true;
            this.cutoff = cutOff;
            timeout = waitTimeout;
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