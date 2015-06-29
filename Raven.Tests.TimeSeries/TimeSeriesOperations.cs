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
				await store.AppendAsync("Time", DateTime.Now, 3D);
				await store.AppendAsync("Time", DateTime.Now, new[] {3D, 4D, 5D, 6D});
				
				var cancellationToken = new CancellationToken();
				await store.AppendAsync("Time", DateTime.Now, 3456D, cancellationToken);
				await store.AppendAsync("Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D }, cancellationToken);
				await store.AppendAsync("Time", DateTime.Now, cancellationToken, 3D, 4D, 5D, 6D);
			}
		}
	}
}