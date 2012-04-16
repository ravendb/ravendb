// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Storage.MultiThreaded;
using Xunit;

namespace Raven.StressTests.Storage.MultiThreadedStress
{
	public class BatchOperationStress : StressTest
	{
		private const int Iterations = 100;

		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<BatchOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), Iterations);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<BatchOperation>(storages => storages.WhenUsingEsentOnDisk(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<BatchOperation>(storages => storages.WhenUsingMuninInMemory(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<BatchOperation>(storages => storages.WhenUsingMuninOnDisk(), Iterations);
		}
	}
}