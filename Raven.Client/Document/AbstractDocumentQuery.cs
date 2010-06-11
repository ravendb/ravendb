using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
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
						    new JsonLuceneDateTimeConverter()
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

		public IDocumentQuery<T> Where(string field, string term, FieldIndexing fieldIndexing)
		{
			string whereClause = field + ":" + Escape(term, fieldIndexing);

			if (string.IsNullOrEmpty(query))
				query = whereClause;
			else
				query += " " + whereClause;

			return this;
		}

		/// <summary>
		/// Escapes Lucene operators and quotes phrases
		/// </summary>
		/// <param name="term"></param>
		/// <returns>escaped term</returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
		/// </remarks>
		private static string Escape(string term, FieldIndexing fieldIndexing)
		{
			if (string.IsNullOrEmpty(term))
			{
				return string.Empty;
			}

			bool isPhrase = false;
			bool isAnalyzed = (fieldIndexing == FieldIndexing.Analyzed);

			int start = 0;
			int length = term.Length;
			StringBuilder builder = null;

			if (!isAnalyzed)
			{
				// NotAnalyzed requires brackets
				builder = new StringBuilder(length*2);
				builder.Append("[[");
			}

			for (int i=start; i<length; i++)
			{
				char ch = term[i];
				switch (ch)
				{
					// should wildcards be included or excluded here?
					case '*':
					case '?':

					case '+':
					case '-':
					case '&':
					case '|':
					case '!':
					case '(':
					case ')':
					case '{':
					case '}':
					case '[':
					case ']':
					case '^':
					case '"':
					case '~':
					case ':':
					case '\\':
					{
						if (builder == null)
						{
							// allocate builder with headroom
							builder = new StringBuilder(length*2);
						}

						if (i > start)
						{
							// append any leading substring
							builder.Append(term, start, i-start);
						}

						builder.Append('\\').Append(ch);
						start = i+1;
						break;
					}
					case ' ':
					case '\t':
					{
						if (isAnalyzed && !isPhrase)
						{
							if (builder == null)
							{
								// allocate builder with headroom
								builder = new StringBuilder(length*2);
							}

							builder.Insert(0, '"');
							isPhrase = true;
						}
						break;
					}
				}
			}

			if (builder == null)
			{
				// no changes required
				return term;
			}

			if (length > start)
			{
				// append any trailing substring
				builder.Append(term, start, length-start);
			}

			if (!isAnalyzed)
			{
				// exact term
				builder.Append("]]");
			}
			else if (isPhrase)
			{
				// quoted phrase
				builder.Append('"');
			}

			return builder.ToString();
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