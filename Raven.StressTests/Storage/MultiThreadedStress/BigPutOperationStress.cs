// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Storage.MultiThreaded;
using Xunit;

namespace Raven.StressTests.Storage.MultiThreadedStress
{
	public class BigPutOperationStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInMemory()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingEsentInMemory());
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingEsentOnDisk());
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingMuninInMemory());
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingMuninOnDisk());
		}
	}
}