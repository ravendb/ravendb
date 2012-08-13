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

namespace Raven.Tests.ManagedStorage
{
	public class MappedResults : TxStorageTest
	{
		[Fact]
		public void CanStoreAndGetMappedResult()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MapRduce.PutMappedResult("test", "users/ayende","ayende", RavenJObject.FromObject(new { Name = "Rahien" })));

				tx.Batch(viewer => Assert.NotEmpty(viewer.MapRduce.GetMappedResults(new GetMappedResultsParams("test", "ayende"))));
			}
		}

		[Fact]
		public void CanDelete()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MapRduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));
				var reduceKeyAndBuckets = new HashSet<ReduceKeyAndBucket>();
				tx.Batch(mutator => mutator.MapRduce.DeleteMappedResultsForDocumentId("users/ayende","test", reduceKeyAndBuckets));

				Assert.NotEmpty(reduceKeyAndBuckets);

				tx.Batch(viewer => Assert.Empty(viewer.MapRduce.GetMappedResults(new GetMappedResultsParams("test", "ayende"))));
			}
		}

		[Fact]
		public void CanDeletePerView()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MapRduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));
				tx.Batch(mutator => mutator.MapRduce.DeleteMappedResultsForView("test"));

				tx.Batch(viewer => Assert.Empty(viewer.MapRduce.GetMappedResults(new GetMappedResultsParams("test", "ayende"))));
			}
		}

		[Fact]
		public void CanHaveTwoResultsForSameDoc()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MapRduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));
				tx.Batch(mutator => mutator.MapRduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new { Name = "Rahien" })));

				tx.Batch(viewer => Assert.Equal(2, viewer.MapRduce.GetMappedResults(new GetMappedResultsParams("test", "ayende")).Count()));
			}
		}

		[Fact]
		public void CanStoreAndGetMappedResultWithSeveralResultsForSameReduceKey()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					mutator.MapRduce.PutMappedResult("test", "users/ayende", "ayende", RavenJObject.FromObject(new {Name = "Rahien"}));
					mutator.MapRduce.PutMappedResult("test", "users/rahien", "ayende", RavenJObject.FromObject(new { Name = "Rahien" }));
				});

				tx.Batch(viewer => Assert.Equal(2, viewer.MapRduce.GetMappedResults(new GetMappedResultsParams("test", "ayende")).Count()));
			}
		}
	}
}