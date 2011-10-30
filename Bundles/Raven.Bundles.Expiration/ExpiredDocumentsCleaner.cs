//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Expiration
{
	public class ExpiredDocumentsCleaner : IStartupTask, IDisposable
	{
		public const string RavenDocumentsByExpirationDate = "Raven/DocumentsByExpirationDate";
		private Logger logger = LogManager.GetCurrentClassLogger();
		private Timer timer;
		public DocumentDatabase Database { get; set; }

		private volatile bool executing;

		public void Execute(DocumentDatabase database)
		{
			Database = database;


			var indexDefinition = database.GetIndexDefinition(RavenDocumentsByExpirationDate);
			if (indexDefinition == null)
			{
				database.PutIndex(RavenDocumentsByExpirationDate,
								  new IndexDefinition
								  {
									  Map =
										  @"
	from doc in docs
	let expiry = doc[""@metadata""][""Raven-Expiration-Date""]
	where expiry != null
	select new { Expiry = expiry }
"
								  });
			}

			var deleteFrequencyInSeconds = database.Configuration.GetConfigurationValue<int>("Raven/Expiration/DeleteFrequencySeconds") ?? 300;
			logger.Info("Initialied expired document cleaner, will check for expired documents every {0} seconds",
						deleteFrequencyInSeconds);
			timer = new Timer(TimerCallback, null, TimeSpan.FromSeconds(deleteFrequencyInSeconds), TimeSpan.FromSeconds(deleteFrequencyInSeconds));

		}

		private void TimerCallback(object state)
		{
			if (executing)
				return;

			executing = true;
			try
			{
				while (true)
				{
					var currentTime = ExpirationReadTrigger.GetCurrentUtcDate();
					var nowAsStr = DateTools.DateToString(currentTime, DateTools.Resolution.SECOND);

					var queryResult = Database.Query(RavenDocumentsByExpirationDate, new IndexQuery
					{
						PageSize = 1024,
						Cutoff = currentTime,
						Query = "Expiry:[* TO " + nowAsStr + "]",
						FieldsToFetch = new[] { "__document_id" }
					});

					if (queryResult.IsStale)
					{
						Thread.Sleep(100);
						continue;
					}

					if (queryResult.Results.Count == 0)
						return;

					var docIds = queryResult.Results.Select(result => result.Value<string>("__document_id")).ToArray();

					logger.Debug(()=> string.Format("Deleting {0} expired documents: [{1}]", queryResult.Results.Count, string.Join(", ", docIds)));

					var deleted = false;
					Database.TransactionalStorage.Batch(accessor => // delete all expired items in a single tx
					{
						deleted = docIds.Aggregate(deleted, (current, docId) => current | Database.Delete(docId, null, null));
					});

					if (deleted == false)
						return;
				}
			}
			catch (Exception e)
			{
				logger.ErrorException("Error when trying to find expired documents", e);
			}
			finally
			{
				executing = false;
			}

		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			if (timer != null)
				timer.Dispose();
		}
	}
}
