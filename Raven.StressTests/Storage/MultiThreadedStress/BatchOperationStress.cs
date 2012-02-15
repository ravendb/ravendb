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
		[Fact]
		public void WhenUsingEsentInMemory()
		{
			Run<BatchOperation>(storages => storages.WhenUsingEsentInMemory());
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<BatchOperation>(storages => storages.WhenUsingEsentOnDisk());
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<BatchOperation>(storages => storages.WhenUsingMuninInMemory());
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<BatchOperation>(storages => storages.WhenUsingMuninOnDisk());
		}
	}
}