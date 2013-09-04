//-----------------------------------------------------------------------
// <copyright file="RavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Connection.Async;
using Raven.Client.Indexes;
using Raven.Client.Spatial;
using Raven.Json.Linq;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Implements <see cref="IRavenQueryable{T}"/>
	/// </summary>
	public class RavenQueryInspector<T> : IRavenQueryable<T>, IRavenQueryInspector
	{
		private readonly Expression expression;
		private readonly IRavenQueryProvider provider;
		private readonly RavenQueryStatistics queryStats;
		private readonly RavenQueryHighlightings highlightings;
		private readonly string indexName;
#if !SILVERLIGHT
		private readonly IDatabaseCommands databaseCommands;
#endif
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private InMemoryDocumentSessionOperations session;
		private readonly bool isMapReduce;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
		/// </summary>
		public RavenQueryInspector(
			IRavenQueryProvider provider, 
			RavenQueryStatistics queryStats,
			RavenQueryHighlightings highlightings,
			string indexName,
			Expression expression,
			InMemoryDocumentSessionOperations session
#if !SILVERLIGHT
			, IDatabaseCommands databaseCommands
#endif
			, IAsyncDatabaseCommands asyncDatabaseCommands,
			bool isMapReduce
			)
		{
			if (provider == null)
			{
				throw new ArgumentNullException("provider");
			}
			this.provider = provider.For<T>();
			this.queryStats = queryStats;
			this.highlightings = highlightings;
			this.indexName = indexName;
			this.session = session;
#if !SILVERLIGHT
			this.databaseCommands = databaseCommands;
#endif
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			this.isMapReduce = isMapReduce;
			this.provider.AfterQueryExecuted(this.AfterQueryExecuted);
			this.expression = expression ?? Expression.Constant(this);
		}

		private void AfterQueryExecuted(QueryResult queryResult)
		{
			this.queryStats.UpdateQueryStats(queryResult);
			this.highlightings.Update(queryResult);
		}

		#region IOrderedQueryable<T> Members

		Expression IQueryable.Expression
		{
			get { return expression; }
		}

		Type IQueryable.ElementType
		{
			get { return typeof(T); }
		}

		IQueryProvider IQueryable.Provider
		{
			get { return provider; }
		}

		/// <summary>
		/// Gets the enumerator.
		/// </summary>
		/// <returns></returns>
		public IEnumerator<T> GetEnumerator()
		{
			var execute = provider.Execute(expression);
			return ((IEnumerable<T>)execute).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		/// <summary>
		/// Provide statistics about the query, such as total count of matching records
		/// </summary>
		public IRavenQueryable<T> Statistics(out RavenQueryStatistics stats)
		{
			stats = queryStats;
			return this;
		}

		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
		/// <returns></returns>
		public IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action)
		{
			provider.Customize(action);
			return this;
		}

	    public IRavenQueryable<TResult> TransformWith<TTransformer, TResult>() where TTransformer : AbstractTransformerCreationTask, new()
	    {
	        var transformer = new TTransformer();
	        provider.TransformWith(transformer.TransformerName);
	        return (IRavenQueryable<TResult>)this.As<TResult>();
	    }

	    public IRavenQueryable<T> AddQueryInput(string input, RavenJToken foo)
	    {
	        provider.AddQueryInput(input, foo);
	        return this;
	    }

		public IRavenQueryable<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
		{
			return Customize(x => x.Spatial(path.ToPropertyPath(), clause));
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
			var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
			string fields = "";
			if (ravenQueryProvider.FieldsToFetch.Count > 0)
				fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
			return fields + luceneQuery;
		}

		public IndexQuery GetIndexQuery(bool isAsync = true)
		{
			RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
			if (isAsync == false)
			{
				var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
				return luceneQuery.GetIndexQuery(false);
			}
			var asyncLuceneQuery = ravenQueryProvider.GetAsyncLuceneQueryFor(expression);
			return asyncLuceneQuery.GetIndexQuery(true);
		}

#if !SILVERLIGHT
		public virtual FacetResults GetFacets(string facetSetupDoc, int start, int? pageSize)
		{
			return databaseCommands.GetFacets(indexName, GetIndexQuery(false), facetSetupDoc, start, pageSize);
		}

		public virtual FacetResults GetFacets(List<Facet> facets, int start, int? pageSize)
		{
			return databaseCommands.GetFacets(indexName, GetIndexQuery(false), facets, start, pageSize);
		}
#endif

		public virtual Task<FacetResults> GetFacetsAsync(string facetSetupDoc, int start, int? pageSize)
		{
			return asyncDatabaseCommands.GetFacetsAsync(indexName, GetIndexQuery(true), facetSetupDoc, start, pageSize);
		}

		public virtual Task<FacetResults> GetFacetsAsync(List<Facet> facets, int start, int? pageSize)
		{
			return asyncDatabaseCommands.GetFacetsAsync(indexName, GetIndexQuery(true), facets, start, pageSize);
		}

		private RavenQueryProviderProcessor<T> GetRavenQueryProvider()
		{
		    return new RavenQueryProviderProcessor<T>(provider.QueryGenerator, provider.CustomizeQuery, null, indexName,
		                                              new HashSet<string>(), new List<RenamedField>(), isMapReduce,
                                                      provider.ResultTransformer, provider.QueryInputs);
		}

		/// <summary>
		/// Get the name of the index being queried
		/// </summary>
		public string IndexQueried
		{
			get
			{
				var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, indexName, new HashSet<string>(), new List<RenamedField>(), isMapReduce, 
                    provider.ResultTransformer, provider.QueryInputs);
				var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
				return ((IRavenQueryInspector)luceneQuery).IndexQueried;
			}
		}

		/// <summary>
		/// Get the name of the index being queried asynchronously
		/// </summary>
		public string AsyncIndexQueried
		{
			get
			{
				var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, indexName, new HashSet<string>(), new List<RenamedField>(), isMapReduce,
                    provider.ResultTransformer, provider.QueryInputs);
				var luceneQuery = ravenQueryProvider.GetAsyncLuceneQueryFor(expression);
				return ((IRavenQueryInspector)luceneQuery).IndexQueried;
			}
		}

#if !SILVERLIGHT
		/// <summary>
		/// Grant access to the database commands
		/// </summary>
		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				if(databaseCommands == null)
					throw new NotSupportedException("You cannot get database commands for this query");
				return databaseCommands;
			}
		}

		
#endif

		/// <summary>
		/// Grant access to the async database commands
		/// </summary>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get
			{
				if (asyncDatabaseCommands == null)
					throw new NotSupportedException("You cannot get database commands for this query");
				return asyncDatabaseCommands;
			}
		}

		public InMemoryDocumentSessionOperations Session
		{
			get
			{
				return session;
			}
		}

		///<summary>
		/// Get the last equality term for the query
		///</summary>
		public KeyValuePair<string, string> GetLastEqualityTerm(bool isAsync = false)
		{
            var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, indexName, new HashSet<string>(), new List<RenamedField>(), isMapReduce, provider.ResultTransformer, provider.QueryInputs);
			if (isAsync)
			{
				var luceneQueryAsync = ravenQueryProvider.GetAsyncLuceneQueryFor(expression);
				return ((IRavenQueryInspector)luceneQueryAsync).GetLastEqualityTerm(true);
			}

			var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
			return ((IRavenQueryInspector) luceneQuery).GetLastEqualityTerm();
		}

#if SILVERLIGHT
		/// <summary>
		///   This function exists solely to forbid calling ToList() on a queryable in Silverlight.
		/// </summary>
		[Obsolete("You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static IList<TOther> ToList<TOther>()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToArray() on a queryable in Silverlight.
		/// </summary>
		[Obsolete("You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static TOther[] ToArray<TOther>()
		{
			throw new NotSupportedException();
		}
#endif

		/// <summary>
		/// Set the fields to fetch
		/// </summary>
		public void FieldsToFetch(IEnumerable<string> fields)
		{
			foreach (var field in fields)
			{
				provider.FieldsToFetch.Add(field);
			}
		}
	}
}
