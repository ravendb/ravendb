//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Expiration
{
	public class ExpiredDocumentsCleaner : IStartupTask, IDisposable
	{
		private const string RavenDocumentsByExpirationDate = "Raven/DocumentsByExpirationDate";

		private Timer timer;
		public DocumentDatabase Database { get; set; }
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
			timer = new Timer(TimerCallback, null, TimeSpan.FromSeconds(deleteFrequencyInSeconds), TimeSpan.FromSeconds(deleteFrequencyInSeconds));

		}

		private void TimerCallback(object state)
		{
			var currentTime = ExpirationReadTrigger.GetCurrentUtcDate();
			var nowAsStr = DateTools.DateToString(currentTime, DateTools.Resolution.SECOND);
			
			var queryResult = Database.Query(RavenDocumentsByExpirationDate, new IndexQuery
			{
				Cutoff = currentTime,
				Query = "Expiry:[* TO " + nowAsStr + "]",
				FieldsToFetch = new[] { "__document_id" }
			});

			foreach (var result in queryResult.Results)
			{
				var docId = result.Value<string>("__document_id");
				Database.Delete(docId, null, null);
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
