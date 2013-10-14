using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;

namespace Raven.Client.Embedded
{
	/// <summary>
	/// The embedded database command for bulk inserts
	/// </summary>
	public class EmbeddedBulkInsertOperation : ILowLevelBulkInsertOperation, IObserver<BulkInsertChangeNotification>
	{
		private CancellationTokenSource cancellationTokenSource;

		private readonly BulkInsertOptions options;
		BlockingCollection<JsonDocument> queue;
		private Task doBulkInsert;

		/// <summary>
		/// Create new instance of this class
		/// </summary>
		public EmbeddedBulkInsertOperation(DocumentDatabase database,BulkInsertOptions options, IDatabaseChanges changes)
		{
			OperationId = Guid.NewGuid();

			this.options = options;
			queue = new BlockingCollection<JsonDocument>(options.BatchSize * 8);

			var cancellationToken = CreateCancellationToken();

			doBulkInsert = Task.Factory.StartNew(() =>
			{
				database.BulkInsert(options, YieldDocuments(cancellationToken), OperationId);
			});

			SubscribeToBulkInsertNotifications(changes);
		}

		private CancellationToken CreateCancellationToken()
		{
			cancellationTokenSource = new CancellationTokenSource();
			return cancellationTokenSource.Token;
		}

		private void SubscribeToBulkInsertNotifications(IDatabaseChanges changes)
		{
			changes
				.ForBulkInsert(OperationId)
				.Subscribe(this);
		}

		private IEnumerable<IEnumerable<JsonDocument>> YieldDocuments(CancellationToken cancellationToken)
		{
			var list = new List<JsonDocument>();
			while (true)
			{
				cancellationToken.ThrowIfCancellationRequested();

				JsonDocument item;
				if (queue.TryTake(out item, 100) == false)
				{
					if (list.Count != 0)
					{
						ReportProgress(list);
						yield return list;
						list.Clear();
					}
					continue;
				}
				if (item == null) //marker
				{
					ReportProgress(list); 
					yield return list; 
					yield break;
				}

				list.Add(item);
				if (list.Count >= options.BatchSize)
				{
					ReportProgress(list); 
					yield return list;
					list.Clear();
				}
			}
		}

		private void ReportProgress(List<JsonDocument> list)
		{
			var onReport = Report;
			if (onReport != null)
				onReport("Writing " + list.Count + " items");
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <returns></returns>
		public Task DisposeAsync()
		{
			Dispose();
			return new CompletedTask();
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			queue.Add(null);
			doBulkInsert.Wait();
			var onReport = Report;
			if (onReport != null)
				onReport("Done with bulk insert");
		}

		/// <summary>
		/// Operation Id
		/// </summary>
		public Guid OperationId { get; private set; }

		/// <summary>
		/// Write the specified data to the database
		/// </summary>
		public void Write(string id, RavenJObject metadata, RavenJObject data)
		{
			if (id == null) throw new ArgumentNullException("id");
			if (metadata == null) throw new ArgumentNullException("metadata");
			if (data == null) throw new ArgumentNullException("data");
			if (doBulkInsert.IsCanceled || doBulkInsert.IsFaulted)
				doBulkInsert.Wait(); // error early

			queue.Add(new JsonDocument
			{
				Key = id,
				DataAsJson = data,
				Metadata = metadata
			});
		}

		/// <summary>
		/// Report on the progress of the operation
		/// </summary>
		public event Action<string> Report;

		/// <summary>
		/// Provides the observer with new data.
		/// </summary>
		/// <param name="value">The current notification information.</param>
		public void OnNext(BulkInsertChangeNotification value)
		{
			if (value.Type == DocumentChangeTypes.BulkInsertError)
			{
				cancellationTokenSource.Cancel();
			}
		}

		/// <summary>
		/// Notifies the observer that the provider has experienced an error condition.
		/// </summary>
		/// <param name="error">An object that provides additional information about the error.</param>
		public void OnError(Exception error)
		{
			
		}

		/// <summary>
		/// Notifies the observer that the provider has finished sending push-based notifications.
		/// </summary>
		public void OnCompleted()
		{
			
		}
	}
}