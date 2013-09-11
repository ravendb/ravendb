// -----------------------------------------------------------------------
//  <copyright file="EtagSynchronizerTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Rhino.Mocks;
using Xunit;

namespace Raven.Tests.Synchronization
{
	public class EtagSynchronizerTests
	{
		private readonly ITransactionalStorage storage;

		private bool saveCalled;

		private int numberOfCalls;

		public EtagSynchronizerTests()
		{
			storage = MockRepository.GenerateStub<ITransactionalStorage>();
			storage.Stub(x => x.Batch(Arg<Action<IStorageActionsAccessor>>.Is.Anything)).WhenCalled(x => numberOfCalls++);
		}

		[Fact]
		public void SynchronizerShouldReturnLowestEtagInEachCycle()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var lowestEtag = EtagUtil.Increment(Etag.Empty, 1);
			var higherEtag = EtagUtil.Increment(Etag.Empty, 2);
			var highestEtag = EtagUtil.Increment(Etag.Empty, 2);

			iSynchronizer.UpdateSynchronizationState(higherEtag);
			iSynchronizer.UpdateSynchronizationState(lowestEtag);
			iSynchronizer.UpdateSynchronizationState(highestEtag);

			var etag = iSynchronizer.GetSynchronizationEtag();
			Assert.Equal(lowestEtag, etag);
		}

		[Fact]
		public void SynchronizerShouldReturnNullIfNoNewEtagsArrivedFromLastGet()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var someEtag = EtagUtil.Increment(Etag.Empty, 1);

			iSynchronizer.UpdateSynchronizationState(someEtag);

			var etag = iSynchronizer.GetSynchronizationEtag();
			Assert.Equal(someEtag, etag);
			Assert.Null(iSynchronizer.GetSynchronizationEtag());
		}

		[Fact]
		public void InitializationShouldLoadLastSynchronizedEtagFromStorage()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			Assert.Equal(1, numberOfCalls);
		}

		[Fact]
		public void Calculation1()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var someEtag = EtagUtil.Increment(Etag.Empty, 1);

			Assert.Equal(someEtag, iSynchronizer.CalculateSynchronizationEtag(null, someEtag));
		}

		[Fact]
		public void Calculation2()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			Assert.Equal(Etag.Empty, iSynchronizer.CalculateSynchronizationEtag(null, null));
		}

		[Fact]
		public void Calculation3()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var someEtag = EtagUtil.Increment(Etag.Empty, 1);

			Assert.Equal(Etag.Empty, iSynchronizer.CalculateSynchronizationEtag(someEtag, null));
		}

		[Fact]
		public void Calculation4()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var lowerEtag = EtagUtil.Increment(Etag.Empty, 1);
			var higherEtag = EtagUtil.Increment(Etag.Empty, 2);

			Assert.Equal(EtagUtil.Increment(lowerEtag, -1), iSynchronizer.CalculateSynchronizationEtag(lowerEtag, higherEtag));
		}

		[Fact]
		public void Calculation5()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var lowerEtag = EtagUtil.Increment(Etag.Empty, 1);
			var higherEtag = EtagUtil.Increment(Etag.Empty, 2);

			Assert.Equal(lowerEtag, iSynchronizer.CalculateSynchronizationEtag(higherEtag, lowerEtag));
		}

		[Fact]
		public void CalculationShouldPersist1()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var lowerEtag = EtagUtil.Increment(Etag.Empty, 1);

			Assert.Equal(lowerEtag, iSynchronizer.CalculateSynchronizationEtag(null, lowerEtag));
			Assert.Equal(2, numberOfCalls);
		}

		[Fact]
		public void CalculationShouldPersist2()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var lowerEtag = EtagUtil.Increment(Etag.Empty, 1);
			var higherEtag = EtagUtil.Increment(Etag.Empty, 2);

			iSynchronizer.UpdateSynchronizationState(higherEtag);

			Assert.Equal(higherEtag, iSynchronizer.GetSynchronizationEtag());

			iSynchronizer.CalculateSynchronizationEtag(null, lowerEtag);

			Assert.Equal(4, numberOfCalls);
		}

		[Fact]
		public void CalculationShouldNotPersist()
		{
			var synchronizer = new DatabaseEtagSynchronizer(storage);
			var iSynchronizer = synchronizer.GetSynchronizer(EtagSynchronizerType.Indexer);

			var lowerEtag = EtagUtil.Increment(Etag.Empty, 1);
			var higherEtag = EtagUtil.Increment(Etag.Empty, 2);

			iSynchronizer.UpdateSynchronizationState(higherEtag);

			Assert.Equal(higherEtag, iSynchronizer.GetSynchronizationEtag());

			iSynchronizer.CalculateSynchronizationEtag(null, higherEtag);

			Assert.Equal(3, numberOfCalls);
		}
	}
}