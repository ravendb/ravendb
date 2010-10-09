using Newtonsoft.Json.Linq;
using Raven.Storage.Managed;
using Xunit;
using System.Linq;

namespace Raven.Storage.Tests
{
	public class MappedResults : TxStorageTest
	{
		[Fact]
		public void CanStoreAndGetMappedResult()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende","ayende", JObject.FromObject(new { Name = "Rahien" }), null));

				tx.Read(viewer => Assert.NotEmpty(viewer.MappedResults.GetMappedResults("test", "ayende", null)));
			}
		}

		[Fact]
		public void CanDelete()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));
				tx.Write(mutator => Assert.NotEmpty(mutator.MappedResults.DeleteMappedResultsForDocumentId("users/ayende","test")));

				tx.Read(viewer => Assert.Empty(viewer.MappedResults.GetMappedResults("test", "ayende", null)));
			}
		}

		[Fact]
		public void CanDeletePerView()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));
				tx.Write(mutator => mutator.MappedResults.DeleteMappedResultsForView("test"));

				tx.Read(viewer => Assert.Empty(viewer.MappedResults.GetMappedResults("test", "ayende", null)));
			}
		}

		[Fact]
		public void CanHaveTwoResultsForSameDoc()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));
				tx.Write(mutator => mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new { Name = "Rahien" }), null));

				tx.Read(viewer => Assert.Equal(2, viewer.MappedResults.GetMappedResults("test", "ayende", null).Count()));
			}
		}

		[Fact]
		public void CanStoreAndGetMappedResultWithSeveralResultsForSameReduceKey()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator =>
				{
					mutator.MappedResults.PutMappedResult("test", "users/ayende", "ayende", JObject.FromObject(new {Name = "Rahien"}),
					                                      null);
					mutator.MappedResults.PutMappedResult("test", "users/rahien", "ayende", JObject.FromObject(new { Name = "Rahien" }),
														  null);
				});

				tx.Read(viewer => Assert.Equal(2, viewer.MappedResults.GetMappedResults("test", "ayende", null).Count()));
			}
		}
	}
}