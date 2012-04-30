//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Batches;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Listeners;
using Raven.Client.Connection;
using Raven.Client.Shard;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document
{
	/// <summary>
	/// A query that is executed against sharded instances
	/// </summary>
	public class AsyncShardedDocumentQuery<T> : AsyncDocumentQuery<T>
	{
		private readonly Func<ShardRequestData, IList<Tuple<string, IAsyncDatabaseCommands>>> getShardsToOperateOn;
		private readonly ShardStrategy shardStrategy;

		private IList<IAsyncDatabaseCommands> databaseCommands;
		private IList<IAsyncDatabaseCommands> ShardDatabaseCommands
		{
			get
			{
				if (databaseCommands == null)
				{
					var shardsToOperateOn = getShardsToOperateOn(new ShardRequestData { EntityType = typeof(T), Query = IndexQuery });
					databaseCommands = shardsToOperateOn.Select(x => x.Item2).ToList();
				}
				return databaseCommands;
			}
		}

		private IndexQuery indexQuery;
		private IndexQuery IndexQuery
		{
			get { return indexQuery ?? (indexQuery = GenerateIndexQuery(theQueryText.ToString())); }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery{T}"/> class.
		/// </summary>
		public AsyncShardedDocumentQuery(InMemoryDocumentSessionOperations session, Func<ShardRequestData, IList<Tuple<string, IAsyncDatabaseCommands>>> getShardsToOperateOn, ShardStrategy shardStrategy, string indexName, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
, null
#endif
, null, indexName, projectionFields, queryListeners)
		{
			this.getShardsToOperateOn = getShardsToOperateOn;
			this.shardStrategy = shardStrategy;
		}

		protected override void InitSync()
		{
			throw new NotImplementedException();
		}

		public override IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
		{
			throw new NotImplementedException();
		}

		protected override void ExecuteActualQuery()
		{
			throw new NotImplementedException();
		}

		protected override Task<QueryOperation> ExecuteActualQueryAsync()
		{
			throw new NotImplementedException();
		}

		public override Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval)
		{
			throw new NotImplementedException();
		}

		public override IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("Sharded has more than one DatabaseCommands to operate on."); }
		}

		public override IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Sharded has more than one DatabaseCommands to operate on."); }
		}
	}
}

#endif
