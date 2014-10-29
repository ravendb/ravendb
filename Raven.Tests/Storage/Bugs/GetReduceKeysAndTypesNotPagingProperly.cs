// -----------------------------------------------------------------------
//  <copyright file="GetReduceKeysAndTypesNotPagingProperly.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Storage.Bugs
{
	using System.Linq;

	using Raven.Database.Storage;

	using Xunit;

	public class GetReduceKeysAndTypesNotPagingProperly : RavenTest
	{
		[Fact]
		public void IssueWithPaging()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "esent"))
			{
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(1, "reduceKey1", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(1, "reduceKey2", ReduceType.SingleStep));
				storage.Batch(accessor => accessor.MapReduce.UpdatePerformedReduceType(1, "reduceKey3", ReduceType.SingleStep));

				storage.Batch(accessor =>
				{
					var reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(1, 0, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k1 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(1, 1, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k2 = reduceKeyAndTypes[0];

					reduceKeyAndTypes = accessor.MapReduce.GetReduceKeysAndTypes(1, 2, 1).ToList();
					Assert.Equal(1, reduceKeyAndTypes.Count);
					var k3 = reduceKeyAndTypes[0];

					Assert.NotEqual(k1.ReduceKey, k2.ReduceKey);
					Assert.NotEqual(k1.ReduceKey, k3.ReduceKey);
					Assert.NotEqual(k2.ReduceKey, k3.ReduceKey);
				});
			}
		} 
	}
}
