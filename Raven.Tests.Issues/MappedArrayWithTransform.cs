using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class MappedArrayWithTransform : RavenTest
	{
		private readonly IDocumentStore docStore;
		private const string RandomUserId = "Users/1";
		private static readonly string[] SampleWatchedProducts = { "Product/1", "Product/2", "Product/3" };

		public MappedArrayWithTransform()
		{
			docStore = NewDocumentStore();

			new UserWatchedProducts().Execute(docStore);
			new UserWatchedProductsWithReduce().Execute(docStore);
			new TestTransformer().Execute(docStore);

			using (var session = docStore.OpenSession())
			{
				session.Store(new UserWatches
				{
					User = RandomUserId,
					Products = SampleWatchedProducts
				});
				session.SaveChanges();
			}
		}

		[Fact]
		public void QueryIndexWithTransformReturnsResultForEachItemInArray()
		{
			using (var session = docStore.OpenSession())
			{
				var results = session.Query<UserWatchedProducts.ReduceResult, UserWatchedProducts>()
					.Customize(x => x.WaitForNonStaleResults())
					.TransformWith<TestTransformer, WatchedProduct>()
					.ToList();

				Assert.Equal(SampleWatchedProducts.Length, results.Count);
			}
		}

		[Fact]
		public void QueryIndexWithReduceAndTransformReturnsResultForEachItemInArray()
		{
			using (var session = docStore.OpenSession())
			{
				var results = session.Query<UserWatchedProductsWithReduce.ReduceResult, UserWatchedProductsWithReduce>()
					.Customize(x => x.WaitForNonStaleResults())
					.TransformWith<TestTransformer, WatchedProduct>()
					.ToList();

				Assert.Equal(SampleWatchedProducts.Length, results.Count);
			}
		}

		public class UserWatches
		{
			public string User { get; set; }
			public string[] Products { get; set; }
		}

		public class UserWatchedProducts : AbstractIndexCreationTask<UserWatches, UserWatchedProducts.ReduceResult>
		{
			public class ReduceResult
			{
				public string User { get; set; }
				public string Product { get; set; }
			}

			public UserWatchedProducts()
			{
				Map = results =>
					from x in results
					from p in x.Products
					select new
					{
						x.User,
						Product = p
					};
			}
		}

		public class UserWatchedProductsWithReduce : AbstractIndexCreationTask<UserWatches, UserWatchedProductsWithReduce.ReduceResult>
		{
			public class ReduceResult
			{
				public string User { get; set; }
				public string Product { get; set; }
			}

			public UserWatchedProductsWithReduce()
			{
				Map = results => from x in results
								 from p in x.Products
								 select new
								 {
									 x.User,
									 Product = p
								 };

				Reduce = results => from result in results
									group result by new { result.User, result.Product }
										into g
										select new
										{
											g.Key.User,
											g.Key.Product
										};
			}
		}

		public class WatchedProduct
		{
			public string User { get; set; }
			public string Description { get; set; }
			public string Product { get; set; }
		}

		public class TestTransformer : AbstractTransformerCreationTask<UserWatchedProducts.ReduceResult>
		{
			public TestTransformer()
			{
				TransformResults = results =>
					from result in results
					select new
					{
						result.User,
						result.Product,
						Description = "Fake loaded product description"
					};
			}
		}

	}
}
