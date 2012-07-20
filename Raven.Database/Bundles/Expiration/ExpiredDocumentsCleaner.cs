//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Expiration
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "Expiration")]
	public class ExpiredDocumentsCleaner : IStartupTask, IDisposable
	{
		public const string RavenDocumentsByExpirationDate = "Raven/DocumentsByExpirationDate";
		private readonly Logger logger = LogManager.GetCurrentClassLogger();
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
				DateTime currentTime = ExpirationReadTrigger.GetCurrentUtcDate();
				string nowAsStr = currentTime.ToString(Default.DateTimeFormatsToWrite);
				logger.Debug("Trying to find expired documents to delete");
				var query = "Expiry:[* TO " + nowAsStr + "]";

				var list = new List<string>();
				int start = 0;
				while (true)
				{
					const int pageSize = 1024;
					var queryResult = Database.Query(RavenDocumentsByExpirationDate, new IndexQuery
					{
						Start = start,
						PageSize = pageSize,
						Cutoff = currentTime,
						Query = query,
						FieldsToFetch = new[] { "__document_id" }
					});

					if(queryResult.Results.Count == 0)
						break;

					start += pageSize;

					list.AddRange(queryResult.Results.Select(result => result.Value<string>("__document_id")).Where(x=>string.IsNullOrEmpty(x) == false));
				}

				if (list.Count == 0)
					return;

				logger.Debug(
					() => string.Format("Deleting {0} expired documents: [{1}]", list.Count, string.Join(", ", list)));

				foreach (var id in list)
				{
					Database.Delete(id, null, null);
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
