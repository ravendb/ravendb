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
		private const int Iterations = 100;

		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<PutOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), Iterations);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<PutOperation>(storages => storages.WhenUsingEsentOnDisk(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<PutOperation>(storages => storages.WhenUsingMuninInMemory(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<PutOperation>(storages => storages.WhenUsingMuninOnDisk(), Iterations);
		}
	}

	public class BigPutOperationStress : StressTest
	{
		private const int Iterations = 100;

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
		private const int Iterations = 100;

		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingEsentInUnreliableMode(), Iterations);
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingEsentOnDisk(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingMuninInMemory(), Iterations);
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			Run<MediumPutOperation>(storages => storages.WhenUsingMuninOnDisk(), Iterations);
		}
	}
}