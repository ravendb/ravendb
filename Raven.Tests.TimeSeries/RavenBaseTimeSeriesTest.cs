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
using Raven.Server;
using Raven.Tests.Helpers;

namespace Raven.Tests.TimeSeries
{
	public class RavenBaseTimeSeriesTest : RavenTestBase
	{
		protected readonly List<TimeSeriesStore> timeSeriesStores = new List<TimeSeriesStore>();

		protected readonly string DefaultTimeSeriesName = "SeriesName#";

		protected RavenBaseTimeSeriesTest()
		{
			foreach (var folder in Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "ThisIsRelativelyUniqueTimeSeriesName*"))
				IOExtensions.DeleteDirectory(folder);

			DefaultTimeSeriesName += Guid.NewGuid();
		}

		protected ITimeSeriesStore NewRemoteTimeSeriesStore(RavenDbServer ravenDbServer = null, bool createDefaultTimeSeries = true, OperationCredentials credentials = null)
		{
			ravenDbServer = GetNewServer(requestedStorage: "voron");

			var timeSeriesStore = new TimeSeriesStore
			{
				Url = GetServerUrl(true, ravenDbServer.SystemDatabase.ServerUrl),
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				Name = DefaultTimeSeriesName + (timeSeriesStores.Count + 1)
			};

			timeSeriesStore.Initialize(createDefaultTimeSeries);
			timeSeriesStores.Add(timeSeriesStore);
			return timeSeriesStore;
		}

		public override void Dispose()
		{
			var errors = new List<Exception>();

			foreach (var store in timeSeriesStores)
			{
				try
				{
					store.Dispose();
				}
				catch (Exception e)
				{
					errors.Add(e);
				}
			}
			stores.Clear();

			if (errors.Count > 0)
				throw new AggregateException(errors);

			base.Dispose();
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
