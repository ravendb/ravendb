// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Storage.MultiThreaded;
using Xunit;

namespace Raven.StressTests.Storage.MultiThreadedStress
{
	public class PutOperationStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInMemory()
		{
			Run<PutOperation>(storages => storages.WhenUsingEsentInMemory());
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<PutOperation>(storages => storages.WhenUsingEsentOnDisk());
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<PutOperation>(storages => storages.WhenUsingMuninInMemory());
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<PutOperation>(storages => storages.WhenUsingMuninOnDisk());
		}
	}
}