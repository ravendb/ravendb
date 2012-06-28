using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanPassTypesProperlyToAggregation 
	{
		[Fact]
		public void WillGenerateDecimalCast()
		{
			Expression<Func<IEnumerable<Coin>, IEnumerable<object>>> query = x => from y in x
			                                                                      group y by y.Denomination
			                                                                      into g
			                                                                      select
			                                                                      	new
			                                                                      	{
			                                                                      		Denomination = g.Key,
			                                                                      		Cost = g.Sum(z => z.Cost)
			                                                                      	};


			var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Coin, Coin>(query, new DocumentConvention(), "docs", false);

			Assert.Equal("docs\r\n\t.GroupBy(y => y.Denomination)\r\n\t.Select(g => new {Denomination = g.Key, Cost = Enumerable.Sum(g, z => ((System.Double)(((double)z.Cost))))})", code);
		}

		[Fact]
		public void WillProperlyCompileWhenUsingToString()
		{
			Expression<Func<IEnumerable<Coin>, IEnumerable<object>>> query = x => from y in x
																				  group y by y.Denomination
																					  into g
																					  select
																						new
																						{
																							Denomination = g.Key,
																							Cost = g.First().Cost.ToString()
																						};


			var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Coin, Coin>(query, new DocumentConvention(), "docs", false);

			Assert.Equal("docs\r\n\t.GroupBy(y => y.Denomination)\r\n\t.Select(g => new {Denomination = g.Key, Cost = ((double)g.First().Cost).ToString()})", code);
		} 
	}
}