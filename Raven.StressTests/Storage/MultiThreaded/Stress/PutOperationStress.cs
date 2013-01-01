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
	}

	public class BigPutOperationStress : StressTest
	{
		// TODO: increase iterations to 10.
		private const int Iterations = 4;

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
	}
}