// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesOperations : RavenBaseTimeSeriesTest
	{
		[Fact]
		public async Task SimpleAppend()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.CreatePrefixConfigurationAsync("-ForValues", 4);

				await store.AppendAsync("-ForValues", "Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D });
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3D);
				
				var cancellationToken = new CancellationToken();
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3456D, cancellationToken);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now, new[] { 23D, 4D, 5D, 6D }, cancellationToken);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now, cancellationToken, 33D, 4D, 5D, 6D);
			}
		}

		[Fact]
		public async Task SimpleAppend_ShouldFailIfTwoKeysAsDifferentValuesLength()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.AppendAsync("-Simple", "Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D });
				await store.AppendAsync("-Simple", "Time", DateTime.Now, 3D);
			}
		}
	}
}