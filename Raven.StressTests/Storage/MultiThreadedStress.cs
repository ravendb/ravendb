// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Storage;
using Xunit;

namespace Raven.StressTests.Storage
{
	public class MultiThreadedStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInMemory()
		{
			Run<MultiThreadedStorages>(storages => storages.WhenUsingEsentInMemory());
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<MultiThreadedStorages>(storages => storages.WhenUsingEsentOnDisk());
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<MultiThreadedStorages>(storages => storages.WhenUsingMuninInMemory());
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<MultiThreadedStorages>(storages => storages.WhenUsingMuninOnDisk());
		}
	}
}