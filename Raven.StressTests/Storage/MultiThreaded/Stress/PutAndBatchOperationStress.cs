// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.StressTests.Storage.MultiThreaded.Stress
{
	public class PutAndBatchOperationStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), 100);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingEsentOnDisk(), 100);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingMuninInMemory(), 300);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingMuninOnDisk(), 300);
		}
	}
}