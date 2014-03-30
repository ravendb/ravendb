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
		private readonly IDatabaseCommands databaseCommands;
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
			, IDatabaseCommands databaseCommands
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
			this.databaseCommands = databaseCommands;
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

        public IRavenQueryable<TResult> TransformWith<TResult>(string transformerName)
        {
            provider.TransformWith(transformerName);
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
			var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
			string fields = "";
			if (ravenQueryProvider.FieldsToFetch.Count > 0)
				fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
			return fields + documentQuery;
		}

		public IndexQuery GetIndexQuery(bool isAsync = true)
		{
			RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
			if (isAsync == false)
			{
				var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
				return documentQuery.GetIndexQuery(false);
			}
			var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
			return asyncDocumentQuery.GetIndexQuery(true);
		}

		public virtual FacetResults GetFacets(string facetSetupDoc, int start, int? pageSize)
		{
			return databaseCommands.GetFacets(indexName, GetIndexQuery(false), facetSetupDoc, start, pageSize);
		}

		public virtual FacetResults GetFacets(List<Facet> facets, int start, int? pageSize)
		{
			return databaseCommands.GetFacets(indexName, GetIndexQuery(false), facets, start, pageSize);
		}

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
				var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
				return ((IRavenQueryInspector)documentQuery).IndexQueried;
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
				var documentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
				return ((IRavenQueryInspector)documentQuery).IndexQueried;
			}
		}

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
				var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
				return ((IRavenQueryInspector)asyncDocumentQuery).GetLastEqualityTerm(true);
			}

			var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
			return ((IRavenQueryInspector) documentQuery).GetLastEqualityTerm();
		}

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
