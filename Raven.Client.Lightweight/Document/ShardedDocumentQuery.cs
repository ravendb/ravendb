//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
#if !NET_3_5
using Raven.Client.Connection.Async;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document.Batches;
#endif
using Raven.Client.Document.SessionOperations;
using Raven.Client.Listeners;
using Raven.Client.Connection;

namespace Raven.Client.Document
{
	/// <summary>
	/// A query that is executed against sharded instances
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class ShardedDocumentQuery<T> : DocumentQuery<T>
	{
		public delegate IDatabaseCommands[] SelectShardsDelegate(string queryAsString);
		private readonly SelectShardsDelegate selectShards;

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="selectShards"></param>
		/// <param name="indexName"></param>
		/// <param name="projectionFields"></param>
		/// <param name="queryListeners"></param>
		public ShardedDocumentQuery(InMemoryDocumentSessionOperations session, SelectShardsDelegate selectShards,
			string indexName, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
			, null
#endif
#if !NET_3_5
			, null
#endif
			, indexName, projectionFields, queryListeners)
		{
			if (selectShards == null)
				throw new ArgumentNullException("selectShards");

			this.selectShards = selectShards;
		}

#if !SILVERLIGHT
		/// <summary>
		///   Gets the enumerator.
		/// </summary>
		public override IEnumerator<T> GetEnumerator()
		{
			InitSync();
			while (true)
			{
				try
				{
					return queryOperations.Complete<T>().GetEnumerator();
				}
				catch (Exception e)
				{
					if (queryOperations.ShouldQueryAgain(e) == false)
						throw;
					ExecuteActualQuery(); // retry the query, not that we explicity not incrementing the session request cuont here
				}
			}
		}
#endif

		protected override QueryOperation queryOperation
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}
		protected IDatabaseCommands[] shards;
		protected readonly List<QueryOperation> queryOperations = new List<QueryOperation>();

		protected override void InitSync()
		{
			if (queryOperations.Count > 0)
				return;
			theSession.IncrementRequestCount();

			var queryText = QueryText.ToString();
			shards = selectShards(queryText);
			foreach (var shardDbCommands in shards)
			{
				foreach (var key in shardDbCommands.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
				{
					shardDbCommands.OperationsHeaders.Remove(key);
				}
				AddQueryOperation(shardDbCommands.OperationsHeaders.Set, queryText);
			}

			// TODO note the difference in where we call listeners here, compared to DocumentSession
			foreach (var documentQueryListener in queryListeners)
			{
				documentQueryListener.BeforeQueryExecuted(this);
			}
			
			ExecuteActualQuery();
		}

		private void AddQueryOperation(Action<string, string> setOperationHeaders, string queryText)
		{
			var indexQuery = GenerateIndexQuery(queryText);
			queryOperations.Add(new QueryOperation(theSession,
												indexName,
												indexQuery,
												projectionFields,
												sortByHints,
												theWaitForNonStaleResults,
												setOperationHeaders,
												timeout));
		}

		protected override void ExecuteActualQuery()
		{
			while (true)
			{
				using (queryOperations.EnterQueryContext())
				{
					queryOperations.LogQuery();
					var result = DatabaseCommands.Query(indexName, queryOperations.IndexQuery, includes.ToArray());
					if (queryOperations.IsAcceptable(result) == false)
					{
						Thread.Sleep(100);
						continue;
					}
					break;
				}
			}
			InvokeAfterQueryExecuted(queryOperations.CurrentQueryResults);
		}

#if !NET_3_5
		protected override Task<QueryOperation> ExecuteActualQueryAsync()
		{
			throw new NotSupportedException();
		}
#endif
	}
}
