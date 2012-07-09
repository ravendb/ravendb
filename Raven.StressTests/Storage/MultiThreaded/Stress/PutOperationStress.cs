// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStress.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.StressTests.Storage.MultiThreaded.Stress
{
	public class PutOperationStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<PutOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), 100);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<PutOperation>(storages => storages.WhenUsingEsentOnDisk(), 50);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<PutOperation>(storages => storages.WhenUsingMuninInMemory(), 200);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<PutOperation>(storages => storages.WhenUsingMuninOnDisk(), 200);
		}
	}

	public class BigPutOperationStress : StressTest
	{
		// TODO: increase iterations to 10.
		private const int Iterations = 1;

		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), Iterations);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingEsentOnDisk(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingMuninInMemory(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<BigPutOperation>(storages => storages.WhenUsingMuninOnDisk(), Iterations);
		}
	}

	public class MediumPutOperationStress : StressTest
	{
		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), 10);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingEsentOnDisk(), 10);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingMuninInMemory(), 10);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingMuninOnDisk(), 10);
		}
	}
}