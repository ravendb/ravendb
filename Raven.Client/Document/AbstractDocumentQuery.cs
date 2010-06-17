using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Client.Linq;

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

		/// <summary>
		/// Matches substrings of the field
		/// </summary>
		/// <param name="fieldName"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public IDocumentQuery<T> Where(string fieldName, object value)
		{
			// default to analyzed fields
			return this.Where(fieldName, value, true);
		}

		/// <summary>
		/// Matches substrings of the field or exact matches if not analyzed
		/// </summary>
		/// <param name="fieldName"></param>
		/// <param name="value"></param>
		/// <param name="isAnalyzed"></param>
		/// <returns></returns>
		public IDocumentQuery<T> Where(string fieldName, object value, bool isAnalyzed)
		{
			string whereClause = fieldName + ":" + TransformToEqualValue(value, isAnalyzed, isAnalyzed);

			if (string.IsNullOrEmpty(query))
				query = whereClause;
			else
				query += " " + whereClause;

			return this;
		}

		/// <summary>
		/// Specifies a boost weight to the last where clause.
		/// The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name="boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		public IDocumentQuery<T> Boost(decimal boost)
		{
			if (string.IsNullOrEmpty(this.query))
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (boost <= 0m)
			{
				throw new ArgumentOutOfRangeException("Boost factor must be a positive number");
			}

			if (boost != 1m)
			{
				// 1.0 is the default
				this.query += "^" + boost;
			}

			return this;
		}

		/// <summary>
		/// Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		public IDocumentQuery<T> Fuzzy(decimal fuzzy)
		{
			if (string.IsNullOrEmpty(this.query))
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (fuzzy < 0m || fuzzy > 1m)
			{
				throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
			}

			char ch = this.query[this.query.Length-1];
			if (ch == '"' || ch == ']')
			{
				// this check is overly simplistic
				throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
			}

			this.query += "~";
			if (fuzzy != 0.5m)
			{
				// 0.5 is the default
				this.query += fuzzy;
			}

			return this;
		}

		/// <summary>
		/// Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name="proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		public IDocumentQuery<T> Proximity(int proximity)
		{
			if (string.IsNullOrEmpty(this.query))
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (proximity < 1)
			{
				throw new ArgumentOutOfRangeException("Proximity distance must be positive number");
			}

			if (this.query[this.query.Length-1] != '"')
			{
				// this check is overly simplistic
				throw new InvalidOperationException("Proximity distance can only modify a phrase");
			}

			this.query += "~" + proximity;

			return this;
		}

		private static string TransformToEqualValue(object value, bool isAnalyzed, bool allowWildcards)
		{
			if (value == null)
				return "NULL_VALUE";

			if (value is DateTime)
				return DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND);

			return LuceneEscape(value.ToString(), isAnalyzed, allowWildcards);
		}

		/// <summary>
		/// Escapes Lucene operators and quotes phrases
		/// </summary>
		/// <param name="term"></param>
		/// <param name="isAnalyzed"></param>
		/// <param name="allowWildcards"></param>
		/// <returns>escaped term</returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
		/// </remarks>
		private static string LuceneEscape(string term, bool isAnalyzed, bool allowWildcards)
		{
			// method doesn't allocate a StringBuilder unless the string requires escaping
			// also this copies chunks of the original string into the StringBuilder which
			// is far more efficient than copying character by character because StringBuilder
			// can access the underlying string data directly

			if (string.IsNullOrEmpty(term))
			{
				return "\"\"";
			}

			bool isPhrase = false;
			int start = 0;
			int length = term.Length;
			StringBuilder buffer = null;

			if (!isAnalyzed)
			{
				// FieldIndexing.NotAnalyzed requires enclosing brackets
				buffer = new StringBuilder(length*2);
				buffer.Append("[[");
			}

			for (int i=start; i<length; i++)
			{
				char ch = term[i];
				switch (ch)
				{
					// should wildcards be included or excluded here?
					case '*':
					case '?':
					{
						if (allowWildcards && isAnalyzed)
						{
							break;
						}
						goto case '\\';
					}
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
						if (buffer == null)
						{
							// allocate builder with headroom
							buffer = new StringBuilder(length*2);
						}

						if (i > start)
						{
							// append any leading substring
							buffer.Append(term, start, i-start);
						}

						buffer.Append('\\').Append(ch);
						start = i+1;
						break;
					}
					case ' ':
					case '\t':
					{
						if (isAnalyzed && !isPhrase)
						{
							if (buffer == null)
							{
								// allocate builder with headroom
								buffer = new StringBuilder(length*2);
							}

							buffer.Insert(0, '"');
							isPhrase = true;
						}
						break;
					}
				}
			}

			if (buffer == null)
			{
				// no changes required
				return term;
			}

			if (length > start)
			{
				// append any trailing substring
				buffer.Append(term, start, length-start);
			}

			if (!isAnalyzed)
			{
				// FieldIndexing.NotAnalyzed requires enclosing brackets
				buffer.Append("]]");
			}
			else if (isPhrase)
			{
				// quoted phrase
				buffer.Append('"');
			}

			return buffer.ToString();
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