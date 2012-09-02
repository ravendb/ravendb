//-----------------------------------------------------------------------
// <copyright file="DynamicFields.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class DynamicFields : RavenTest
	{
		public class Product
		{
			public string Id { get; set; }
			public List<Attribute> Attributes { get; set; }
		}

		public class Attribute
		{
			public string Name { get; set; }
			public string Value { get; set; }
			public double NumericValue { get; set; }
			public int IntValue { get; set; }
		}

		public class Product_ByAttribute : AbstractIndexCreationTask<Product>
		{
			public Product_ByAttribute()
			{
				Map = products =>
					from p in products
					select new
					{
						_ = p.Attributes.Select(attribute => new Field(attribute.Name, attribute.Value, Field.Store.NO, Field.Index.ANALYZED))
					};
			}
		}

		public class Product_ByNumericAttribute : AbstractIndexCreationTask<Product>
		{
			public Product_ByNumericAttribute()
			{
				Map = products =>
					from p in products
					select new
					{
						_ = p.Attributes.Select(attribute => new NumericField(attribute.Name+"_Range", Field.Store.NO, true).SetDoubleValue(attribute.NumericValue))
					};
			}
		}

		public class Product_ByNumericAttributeUsingField : AbstractIndexCreationTask<Product>
		{
			public Product_ByNumericAttributeUsingField()
			{
				Map = products =>
					from p in products
					select new
					{
						_ = p.Attributes.Select(attribute => new Field(attribute.Name, attribute.NumericValue.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS))
					};
			}
		}

		public class Product_ByIntAttribute : AbstractIndexCreationTask<Product>
		{
			public Product_ByIntAttribute()
			{
				Map = products =>
					from p in products
					select new
					{
						_ = p.Attributes.Select(attribute => new NumericField(attribute.Name +"_Range", Field.Store.NO, true).SetIntValue(attribute.IntValue))
					};
			}
		}


		[Fact]
		public void CanCreateCompletelyDynamicFields()
		{
			using (var store = NewDocumentStore())
			{
				new Product_ByAttribute().Execute(((IDocumentStore) store).DatabaseCommands, ((IDocumentStore) store).Conventions);

				using (var session = store.OpenSession())
				{
					session.Store(new Product
					{
						Attributes = new List<Attribute>
						{
							new Attribute{Name = "Color", Value = "Red"}
						}
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var products = session.Advanced.LuceneQuery<Product>("Product/ByAttribute")
						.WhereEquals("Color", "Red")
						.WaitForNonStaleResults(TimeSpan.FromMinutes(3))
						.ToList();

					Assert.NotEmpty(products);
				}
			}
		}

		[Fact]
		public void CanCreateCompletelyDynamicNumericFields()
		{
			using (var store = NewDocumentStore())
			{
				new Product_ByNumericAttribute().Execute(((IDocumentStore) store).DatabaseCommands, ((IDocumentStore) store).Conventions);

				using (var session = store.OpenSession())
				{
					session.Store(new Product
					{
						Attributes = new List<Attribute>
						{
							new Attribute{Name = "Color", Value = "Red", NumericValue = 30}
						}
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var products = session.Advanced.LuceneQuery<Product, Product_ByNumericAttribute>()
						.WhereGreaterThan("Color", 20d)
						.WaitForNonStaleResults(TimeSpan.FromMinutes(3))
						.ToList();

					Assert.NotEmpty(products);
				}
			}
		}

		[Fact]
		public void CanCreateCompletelyDynamicNumericFieldsUsingField()
		{
			using (var store = NewDocumentStore())
			{
				new Product_ByNumericAttributeUsingField().Execute(((IDocumentStore)store).DatabaseCommands, ((IDocumentStore)store).Conventions);

				using (var session = store.OpenSession())
				{
					session.Store(new Product
					{
						Attributes = new List<Attribute>
						{
							new Attribute{Name = "Color", Value = "Red", NumericValue = 30}
						}
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var products = session.Advanced.LuceneQuery<Product, Product_ByNumericAttributeUsingField>()
						.WhereEquals("Color", 30d)
						.WaitForNonStaleResults(TimeSpan.FromMinutes(3))
						.ToList();

					Assert.NotEmpty(products);
				}
			}
		}

		[Fact]
		public void CanQueryCompletelyDynamicNumericFieldsWithNegativeRangeUsingInt()
		{
			using (var store = NewDocumentStore())
			{
				new Product_ByIntAttribute().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Product
					{
						Attributes = new List<Attribute>
						{
							new Attribute{Name = "Color", Value = "Red", IntValue = 30}
						}
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var products = session.Advanced.LuceneQuery<Product, Product_ByIntAttribute>()
						.WhereGreaterThan("Color", -1)
						.WaitForNonStaleResults(TimeSpan.FromMinutes(3))
						.ToList();

					Assert.NotEmpty(products);
				}
			}
		}

		[Fact]
		public void CanQueryCompletelyDynamicNumericFieldsWithNegativeRange()
		{
			using (var store = NewDocumentStore())
			{
				new Product_ByNumericAttribute().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Product
					{
						Attributes = new List<Attribute>
						{
							new Attribute{Name = "Color", Value = "Red", NumericValue = 30}
						}
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var products = session.Advanced.LuceneQuery<Product, Product_ByNumericAttribute>()
						.WhereGreaterThan("Color", -1d)
						.WaitForNonStaleResults(TimeSpan.FromMinutes(3))
						.ToList();

					Assert.NotEmpty(products);
				}
			}
		}
	}


}
