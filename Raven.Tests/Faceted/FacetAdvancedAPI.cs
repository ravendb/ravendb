using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Faceted
{
	public class Test
	{
		public String Id { get; set; }
		public String Manufacturer { get; set; }
		public DateTime Date { get; set; }
		public Decimal Cost { get; set; }
		public int Quantity { get; set; }
		public Double Price { get; set; }
	}

	public class FacetAdvancedAPI
	{
		[Fact]
		public void CanUseNewAPIToDoMultipleQueries()
		{
			var oldFacets = new List<Facet>
			{
				new Facet {Name = "Manufacturer"},
				new Facet
				{
					Name = "Cost_Range",
					Mode = FacetMode.Ranges,
					Ranges =
					{
						"[NULL TO Dx200.0]",
						"[Dx200.0 TO Dx400.0]",
						"[Dx400.0 TO Dx600.0]",
						"[Dx600.0 TO Dx800.0]",
						"[Dx800.0 TO NULL]",
					}
				},
				new Facet
				{
					Name = "Price_Range",
					Mode = FacetMode.Ranges,
					Ranges =
					{
						"[NULL TO Dx9.99]",
						"[Dx9.99 TO Dx49.99]",
						"[Dx49.99 TO Dx99.99]",
						"[Dx99.99 TO NULL]",
					}
				}
			};

			var newFacets = new List<Facet>
			{
				new Facet<Test> {Name = x => x.Manufacturer},
				new Facet<Test>
				{  
					Name = x => x.Cost,
					Ranges =
						{
							x => x.Cost < 200m,
							x => x.Cost > 200m && x.Cost < 400m,
							x => x.Cost > 400m && x.Cost < 600m,
							x => x.Cost > 600m && x.Cost < 800m,
							x => x.Cost > 800m
						}
				},
				new Facet<Test>
				{  
					Name = x => x.Price,
					Ranges = 
					{
						x => x.Price < 9.99,
						x => x.Price > 9.99 && x.Price < 49.99, 
						x => x.Price > 49.99 && x.Price < 99.99, 
						x => x.Price > 99.99
					}
				},                
			};

			Assert.Equal(true, AreFacetsEqual(oldFacets[0], newFacets[0]));
			Assert.Equal(true, AreFacetsEqual(oldFacets[1], newFacets[1]));
			Assert.Equal(true, AreFacetsEqual(oldFacets[2], newFacets[2]));
		}

		[Fact]
		public void NewAPIThrowsExceptionsForInvalidExpressions()
		{
			//Create an invalid lamba and check it throws an exception!!			
			Assert.Throws<InvalidOperationException>(() => 
				TriggerConversion(new Facet<Test>
				{
					Name = x => x.Cost,
					//Ranges can be a single item or && only
					Ranges = { x => x.Cost > 200m || x.Cost < 400m }
				}));

			Assert.Throws<InvalidOperationException>(() =>
				TriggerConversion(new Facet<Test>
				{
					Name = x => x.Cost,
					//Ranges can be > or < only
					Ranges = { x => x.Cost == 200m }
				}));

			Assert.Throws<InvalidOperationException>(() =>
				TriggerConversion(new Facet<Test>
				{
					//Facets must contain a Name expression
					//Name = x => x.Cost,
					Ranges = { x => x.Cost > 200m }
				}));
		}        

		private bool AreFacetsEqual(Facet left, Facet right)
		{
			return left.Name == right.Name &&
				left.Mode == right.Mode &&
				left.Ranges.Count == right.Ranges.Count &&
				left.Ranges.All(x => left.Ranges.Contains(x));
		}

		private Facet TriggerConversion(Facet<Test> facet)
		{
			//The conversion is done with an implicit cast, 
			//so we remain compatible with the original facet API
			return (Facet)facet;
		}
	}
}
