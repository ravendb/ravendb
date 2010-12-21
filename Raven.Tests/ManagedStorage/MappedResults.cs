//-----------------------------------------------------------------------
// <copyright file="MappedResults.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;
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
				tx.Batch(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende","ayende", JObject.FromObject(new { Name = "Rahien" }), null));

				tx.Batch(viewer => Assert.NotEmpty(viewer.MappedResults.GetMappedResults("test", "ayende", null)));
			}
		}

		[Fact]
		public void CanDelete()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));
				tx.Batch(mutator => Assert.NotEmpty(mutator.MappedResults.DeleteMappedResultsForDocumentId("users/ayende","test")));

				tx.Batch(viewer => Assert.Empty(viewer.MappedResults.GetMappedResults("test", "ayende", null)));
			}
		}

		[Fact]
		public void CanDeletePerView()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));
				tx.Batch(mutator => mutator.MappedResults.DeleteMappedResultsForView("test"));

				tx.Batch(viewer => Assert.Empty(viewer.MappedResults.GetMappedResults("test", "ayende", null)));
			}
		}

		[Fact]
		public void CanHaveTwoResultsForSameDoc()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));
				tx.Batch(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));

				tx.Batch(viewer => Assert.Equal(2, viewer.MappedResults.GetMappedResults("test", "ayende", null).Count()));
			}
		}

		[Fact]
		public void CanStoreAndGetMappedResultWithSeveralResultsForSameReduceKey()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new {Name = "Rahien"}),
					                                      null);
					mutator.MappedResults.PutMappedResult("test", "users/rahien", "ayende", JObject.FromObject(new { Name = "Rahien" }),
														  null);
				});

				tx.Batch(viewer => Assert.Equal(2, viewer.MappedResults.GetMappedResults("test", "ayende", null).Count()));
			}
		}
	}
}