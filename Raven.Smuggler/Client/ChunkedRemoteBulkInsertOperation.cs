// -----------------------------------------------------------------------
//  <copyright file="ChunkedRemoteBulkInsertOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Util;
using Raven.Json.Linq;

namespace Raven.Smuggler.Client
{
	public class ChunkedRemoteBulkInsertOperation : ILowLevelBulkInsertOperation
	{
		private readonly BulkInsertOptions options;

		private readonly AsyncServerClient client;

		private readonly IDatabaseChanges changes;

		private readonly int chunkSize;

		private int processedItemsInCurrentOperation;

		private RemoteBulkInsertOperation current;

		private readonly long? documentSizeInChunkLimit;

		private long documentSizeInChunk;

		private bool disposed;

		private Task<int> previousTask;

		public ChunkedRemoteBulkInsertOperation(BulkInsertOptions options, AsyncServerClient client, IDatabaseChanges changes, int chunkSize,long? documentSizeInChunkLimit = null)
		{
			this.options = options;
			this.client = client;
			this.changes = changes;
			this.chunkSize = chunkSize;
			this.documentSizeInChunkLimit = documentSizeInChunkLimit;
			documentSizeInChunk = 0;
		}

		public Guid OperationId
		{
			get
			{
				return current == null ? Guid.Empty : current.OperationId;
			}
		}

		public void Write(string id, RavenJObject metadata, RavenJObject data, int? dataSize)
		{
			current = GetBulkInsertOperation();

			current.Write(id, metadata, data, dataSize);

			if(documentSizeInChunkLimit.HasValue)
				documentSizeInChunk += DocumentHelpers.GetRoughSize(data);

			processedItemsInCurrentOperation++;
		}

		private RemoteBulkInsertOperation GetBulkInsertOperation()
		{
			if (current == null)
				return current = CreateBulkInsertOperation(Task.FromResult(0));

			if (processedItemsInCurrentOperation < chunkSize)
				if (!documentSizeInChunkLimit.HasValue || documentSizeInChunk < documentSizeInChunkLimit.Value)
					return current;

			// if we haven't flushed the previous one yet, we will force 
			// a disposal of both the previous one and the one before, to avoid 
			// consuming a lot of memory, and to have _too_ much concurrency.
			if (previousTask != null)
			{
				previousTask.ConfigureAwait(false).GetAwaiter().GetResult();
			}
			previousTask = current.DisposeAsync();

			documentSizeInChunk = 0;
			processedItemsInCurrentOperation = 0;
			current = CreateBulkInsertOperation(previousTask);
			return current;
		}

		private RemoteBulkInsertOperation CreateBulkInsertOperation(Task<int> disposeAsync)
		{
			var operation = new RemoteBulkInsertOperation(options, client, changes, disposeAsync);
			if (Report != null)
				operation.Report += Report;

			return operation;
		}

		public async Task<int> DisposeAsync()
		{
			if (disposed)
				return -1;

			disposed = true;

			if (current != null)
				return await (current.DisposeAsync());

			return 0;
		}

		public event Action<string> Report;
	    public void Abort()
	    {
	        current.Abort();
	    }

	    public void Dispose()
		{
			if (disposed)
				return;

			using (NoSynchronizationContext.Scope())
			{
				var disposeAsync = DisposeAsync().ConfigureAwait(false);
				disposeAsync.GetAwaiter().GetResult();
			}
		}


		public bool IsAborted
		{
			get { return current.IsAborted; }
		}
	}
}