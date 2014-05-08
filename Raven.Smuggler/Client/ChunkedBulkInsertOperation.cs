// -----------------------------------------------------------------------
//  <copyright file="ChunkedBulkInsertOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Client.Document;

namespace Raven.Smuggler.Client
{
	public class ChunkedBulkInsertOperation : BulkInsertOperation
	{
		public ChunkedBulkInsertOperation(string database, IDocumentStore documentStore, DocumentSessionListeners listeners, BulkInsertOptions options, IDatabaseChanges changes, int chunkSize)
			: base(database, documentStore, listeners, options, changes)
		{
			Operation = new ChunkedRemoteBulkInsertOperation(options, (AsyncServerClient)DatabaseCommands, changes, chunkSize);
		}

		protected override ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IAsyncDatabaseCommands commands, IDatabaseChanges changes)
		{
			return null; // ugly code
		}
	}
}