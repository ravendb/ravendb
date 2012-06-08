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
						Name = "Megapixels_Range",
						Mode = FacetMode.Ranges,
						Ranges =
							{
								"[NULL TO Dx3.0]",
								"[Dx3.0 TO Dx7.0]",
								"[Dx7.0 TO Dx10.0]",
								"[Dx10.0 TO NULL]",
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
					}
			};

			Assert.Equal(true, AreFacetsEqual(oldFacets[0], newFacets[0]));
			Assert.Equal(true, AreFacetsEqual(oldFacets[1], newFacets[1]));
		}

		[Fact]
		public void NewAPIThrowsExceptionsForInvalidExpressions()
		{
			//Create an invalid lamba and check it throws an exception!!
		}

		private bool AreFacetsEqual(Facet left, Facet right)
		{
			return left.Name == right.Name &&
				left.Mode == right.Mode &&
				left.Ranges.Count == right.Ranges.Count &&
				left.Ranges.All(x => left.Ranges.Contains(x));
		}
	}
}
