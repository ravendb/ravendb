// -----------------------------------------------------------------------
//  <copyright file="ChunkedRemoteBulkInsertOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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

		private readonly IList<Task> tasks = new List<Task>();

		private readonly long? documentSizeInChunkLimit;

		private long documentSizeInChunk;

		private bool disposed;

		public ChunkedRemoteBulkInsertOperation(BulkInsertOptions options, AsyncServerClient client, IDatabaseChanges changes, int chunkSize,long? documentSizeInChunkLimit = null)
		{
			this.options = options;
			this.client = client;
			this.changes = changes;
			this.chunkSize = chunkSize;
			this.documentSizeInChunkLimit = documentSizeInChunkLimit;
			documentSizeInChunk = 0;
			if(documentSizeInChunkLimit.HasValue)
				Console.WriteLine("Limit of document size in chunk = " + documentSizeInChunkLimit.Value);
		}

		public Guid OperationId
		{
			get
			{
				return current == null ? Guid.Empty : current.OperationId;
			}
		}

		public void Write(string id, RavenJObject metadata, RavenJObject data)
		{
			current = GetBulkInsertOperation();

			current.Write(id, metadata, data);

			if(documentSizeInChunkLimit.HasValue)
				documentSizeInChunk += DocumentHelpers.GetRoughSize(data);

			processedItemsInCurrentOperation++;
		}

		private RemoteBulkInsertOperation GetBulkInsertOperation()
		{
			if (current == null)
				return current = CreateBulkInsertOperation();

			//
			if (processedItemsInCurrentOperation < chunkSize)
				if (!documentSizeInChunkLimit.HasValue || documentSizeInChunk < documentSizeInChunkLimit.Value)
					return current;

			documentSizeInChunk = 0;
			processedItemsInCurrentOperation = 0;
			tasks.Add(current.DisposeAsync());
			return current = CreateBulkInsertOperation();
		}

		private RemoteBulkInsertOperation CreateBulkInsertOperation()
		{
			var operation = new RemoteBulkInsertOperation(options, client, changes);
			if (Report != null)
				operation.Report += Report;

			return operation;
		}

		public async Task DisposeAsync()
		{
			if (disposed)
				return;

			disposed = true;

			if (current != null)
				tasks.Add(current.DisposeAsync());

			await Task.WhenAll(tasks);
		}

		public event Action<string> Report;

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
	}
}