using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_478 : RavenTest
	{
		public class ProductContent
		{
			public string Slug { get; set; }
			public string Title { get; set; }
			public string Content { get; set; }
		}
		public class Product
		{
			public string ShortName { get; set; }
			public string Name { get; set; }
			public string Summary { get; set; }
			public string Description { get; set; }
			public string HomepageContent { get; set; }
		}

		public class ContentSearchIndex : AbstractMultiMapIndexCreationTask<ContentSearchIndex.Result>
		{
			public class Result
			{
				public string Slug { get; set; }
				public string Title { get; set; }
				public string Content { get; set; }
			}

			public ContentSearchIndex()
			{
				AddMap<ProductContent>(docs => from content in docs
											   select new { Slug = content.Slug.Boost(15), Title = content.Title.Boost(13), content.Content });

				AddMap<Product>(docs => from product in docs
				                        select
					                        new
					                        {
						                        Slug = product.ShortName.Boost(30),
						                        Title = product.Name.Boost(25),
						                        Content =
					                        new string[] {product.Summary, product.Description, product.HomepageContent}.Boost(20)
					                        });


				Index(x => x.Slug, FieldIndexing.Analyzed);
				Index(x => x.Title, FieldIndexing.Analyzed);
				Index(x => x.Content, FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void IndexWithBoost()
		{
			using(var store = NewDocumentStore())
			{
				new ContentSearchIndex().Execute(store);
			}
		}
	}
}