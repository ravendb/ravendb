using Raven.Abstractions.Indexing;

namespace Raven.Tests.Bugs.LiveProjections.Indexes
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Linq.Expressions;

	using Raven.Client.Indexes;
	using Raven.Database.Indexing;
	using Raven.Tests.Bugs.LiveProjections.Entities;
	using Raven.Tests.Bugs.LiveProjections.Views;

	public class ProductSkuListViewModelReport_ByArticleNumberAndName : AbstractIndexCreationTask<ProductSku, ProductSkuListViewModelReport>
	{
		public ProductSkuListViewModelReport_ByArticleNumberAndName()
		{
			Map = products => from product in products
			                  select new
			                  	{
			                  		Id = product.Id,
			                  		ArticleNumber = product.ArticleNumber,
			                  		Name = product.Name
			                  	};

			TransformResults = (database, results) =>
			                   from result in results
			                   let product = database.Load<ProductSku>(result.Id)
			                   let stock = database.Load<ProductSku>(result.Id)
			                   select new
			                   	{
			                   		result.Id,
			                   		result.ArticleNumber,
			                   		result.Name,
			                   		product.Packing,
			                   		stock.QuantityInWarehouse
			                   	};

			Indexes = new Dictionary<Expression<Func<ProductSkuListViewModelReport, object>>, FieldIndexing>()
			    {
			        { e=>e.ArticleNumber, FieldIndexing.Analyzed},
			        { e=>e.Name, FieldIndexing.Analyzed}
			    };
		}
	}
}