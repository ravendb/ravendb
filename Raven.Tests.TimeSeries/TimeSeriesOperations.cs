// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Tests.Common;
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
		public async Task ShouldNotAllowOverwritePrefix()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.CreatePrefixConfigurationAsync("-Simple", 2));
				Assert.Contains("System.InvalidOperationException: Prefix -Simple is already created", exception.Message);
			}
		}

		[Fact]
		public async Task SimpleAppend_ShouldFailIfTwoKeysAsDifferentValuesLength()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.AppendAsync("-Simple", "Time", DateTime.Now, 3D);

				var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.AppendAsync("-Simple", "Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D }));
				Assert.Contains("System.ArgumentOutOfRangeException: Appended values should be the same length the series values length which is 1 and not 4", exception.Message);
			}
		}
	}
}