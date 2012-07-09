// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.StressTests.Storage.MultiThreaded.Stress
{
	public class BatchOperationStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<BatchOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), 10);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<BatchOperation>(storages => storages.WhenUsingEsentOnDisk(), 10);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<BatchOperation>(storages => storages.WhenUsingMuninInMemory(), 10);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<BatchOperation>(storages => storages.WhenUsingMuninOnDisk(), 10);
		}
	}
}