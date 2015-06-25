using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.TimeSeries;
using Raven.Client;
using Raven.Client.TimeSeries;
using Raven.Database.Extensions;
using Raven.Tests.Helpers;

namespace Raven.Tests.TimeSeries
{
	public class RavenBaseTimeSeriesTest : RavenTestBase
	{
		protected readonly IDocumentStore ravenStore;
		private readonly ConcurrentDictionary<string, int> storeCount;
		protected readonly string DefaultTimeSeriesName = "ThisIsRelativelyUniqueTimeSeriesName";

		protected RavenBaseTimeSeriesTest()
		{
			foreach (var folder in Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "ThisIsRelativelyUniqueTimeSeriesName*"))
				IOExtensions.DeleteDirectory(folder);

			ravenStore = NewRemoteDocumentStore(fiddler:true);
			DefaultTimeSeriesName += Guid.NewGuid();
			storeCount = new ConcurrentDictionary<string, int>();
		}

		protected ITimeSeriesStore NewRemoteTimeSeriesStore(string timeSeriesName, bool createDefaultTimeSeries = true,OperationCredentials credentials = null, IDocumentStore ravenStore = null)
		{
			ravenStore = ravenStore ?? this.ravenStore;
			storeCount.AddOrUpdate(ravenStore.Identifier, id => 1, (id, val) => val++);		
	
			var timeSeriesStore = new TimeSeriesStore
			{
				Url = ravenStore.Url,
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				Name = timeSeriesName + storeCount[ravenStore.Identifier]
			};
			timeSeriesStore.Initialize(createDefaultTimeSeries);
			return timeSeriesStore;
		}

		protected TimeSeriesDocument CreateTimeSeriesDocument(string timeSeriesName)
		{
			return new TimeSeriesDocument
			{
				Settings = new Dictionary<string, string>
				{
					{ "Raven/TimeSeries/DataDir", @"~\TimeSeries\" + timeSeriesName }
				},
			};
		}

		public override void Dispose()
		{
			if (ravenStore != null) ravenStore.Dispose();

			try
			{
				base.Dispose();
			}
			catch (AggregateException) //TODO: do not forget to investigate where time series is not being disposed
			{
			}
		}

		protected async Task<bool> WaitForReplicationBetween(ITimeSeriesStore source, ITimeSeriesStore destination, string groupName, string timeSeriesName, int timeoutInSec = 30)
		{
			var waitStartingTime = DateTime.Now;
			var hasReplicated = false;

			if (Debugger.IsAttached)
				timeoutInSec = 60 * 60; //1 hour timeout if debugging

			while (true)
			{
				if ((DateTime.Now - waitStartingTime).TotalSeconds > timeoutInSec)
					break;

				var sourceValue = await source.GetOverallTotalAsync(groupName, timeSeriesName);
				var targetValue = await destination.GetOverallTotalAsync(groupName, timeSeriesName);
				if (sourceValue == targetValue)
				{
					hasReplicated = true;
					break;
				}

				Thread.Sleep(50);
			}

			return hasReplicated;
		}

		protected static async Task SetupReplicationAsync(ITimeSeriesStore source, params ITimeSeriesStore[] destinations)
		{
			var replicationDocument = new TimeSeriesReplicationDocument();
			foreach (var destStore in destinations)
			{
				replicationDocument.Destinations.Add(new TimeSeriesReplicationDestination
				{
					TimeSeriesName = destStore.Name,
					ServerUrl = destStore.Url
				});
			}

			await source.SaveReplicationsAsync(replicationDocument);
		}
	}
}
