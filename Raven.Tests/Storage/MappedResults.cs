//-----------------------------------------------------------------------
// <copyright file="MappedResults.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Json.Linq;
using Raven.Database.Storage;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class MappedResults : RavenTest
	{
		[Fact]
		public void CanStoreAndGetMappedResult()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.MapReduce.PutMappedResult("test", "users/ayende","ayende", RavenJObject.FromObject(new { Name = "Rahien" })));

				tx.BatchRead(viewer => Assert.NotEmpty(viewer.MapReduce.GetMappedResultsForDebug("test", "ayende",0, 100)));
			}
		}

		[Fact]
		public void CanDelete()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.MapReduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));
				var reduceKeyAndBuckets = new Dictionary<ReduceKeyAndBucket, int>();
				tx.BatchRead(mutator => mutator.MapReduce.DeleteMappedResultsForDocumentId("users/ayende","test", reduceKeyAndBuckets));

				Assert.NotEmpty(reduceKeyAndBuckets);

				tx.BatchRead(viewer => Assert.Empty(viewer.MapReduce.GetMappedResultsForDebug("test", "ayende",0, 100)));
			}
		}

		[Fact]
		public void CanDeletePerView()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.MapReduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));
				tx.BatchRead(mutator => mutator.MapReduce.DeleteMappedResultsForView("test"));

				tx.BatchRead(viewer => Assert.Empty(viewer.MapReduce.GetMappedResultsForDebug("test", "ayende", 0, 100)));
			}
		}

		[Fact]
		public void CanHaveTwoResultsForSameDoc()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator => mutator.MapReduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));
				tx.BatchRead(mutator => mutator.MapReduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));

				tx.BatchRead(viewer => Assert.Equal(2, viewer.MapReduce.GetMappedResultsForDebug("test", "ayende",0, 100).Count()));
			}
		}

		[Fact]
		public void CanStoreAndGetMappedResultWithSeveralResultsForSameReduceKey()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.BatchRead(mutator =>
				{
					mutator.MapReduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new {Name = "Rahien"}));
					mutator.MapReduce.PutMappedResult("test", "users/rahien", "ayende", RavenJObject.FromObject(new { Name = "Rahien" }));
				});

				tx.BatchRead(viewer => Assert.Equal(2, viewer.MapReduce.GetMappedResultsForDebug("test", "ayende",0, 100).Count()));
			}
		}
	}
}