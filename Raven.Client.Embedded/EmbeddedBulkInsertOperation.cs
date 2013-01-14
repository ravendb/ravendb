using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Tasks;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;

namespace Raven.Client.Embedded
{
	/// <summary>
	/// The embedded database command for bulk inserts
	/// </summary>
	public class EmbeddedBulkInsertOperation : ILowLevelBulkInsertOperation
	{
		private readonly BulkInsertOptions options;
		BlockingCollection<JsonDocument> queue;
		private Task doBulkInsert;

		/// <summary>
		/// Create new instance of this class
		/// </summary>
		public EmbeddedBulkInsertOperation(DocumentDatabase database,BulkInsertOptions options)
		{
			this.options = options;
			queue = new BlockingCollection<JsonDocument>(options.BatchSize * 8);
			doBulkInsert = Task.Factory.StartNew(() =>
			{
				database.BulkInsert(options, YieldDocuments());
			});
		}

		private IEnumerable<IEnumerable<JsonDocument>> YieldDocuments()
		{
			var list = new List<JsonDocument>();
			while (true)
			{
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
		public void Dispose()
		{
			queue.Add(null);
			doBulkInsert.Wait();
			var onReport = Report;
			if (onReport != null)
				onReport("Done with bulk insert");
		}

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
	}
}