// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Storage.MultiThreaded;
using Xunit;

namespace Raven.StressTests.Storage.MultiThreadedStress
{
	public class PutAndBatchOperationStress : StressTest
	{
		private const int Iterations = 100;

		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), Iterations);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingEsentOnDisk(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingMuninInMemory(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<PutAndBatchOperation>(storages => storages.WhenUsingMuninOnDisk(), Iterations);
		}
	}
}