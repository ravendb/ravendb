// -----------------------------------------------------------------------
//  <copyright file="OfTypeSupport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
	public class OfTypeSupport : RavenTest
	{
		[Fact]
		public void OfTypeWillBeConvertedToWhere()
		{
			using (var store = NewDocumentStore())
			{
				new TagSummaryIndex().Execute(store);
			}
		}

		public class Foo
		{
			public string Tag { get; set; }
			public List<Bar> Bars { get; set; }
		}

		public abstract class Bar
		{
			public int Weight { get; set; }
		}

		public class IronBar : Bar { }

		public class ChocolateBar : Bar { }

		public class TagSummary
		{
			public string Tag { get; set; }
			public int Count { get; set; }
			public int TotalChocolateBarWeight { get; set; }
			public int TotalIronBarWeight { get; set; }
		}

		public class TagSummaryIndex : AbstractIndexCreationTask<Foo, TagSummary>
		{
			public TagSummaryIndex()
			{
				Map = foos => from f in foos
							  select new
							  {
								  Tag = f.Tag,
								  Count = 1,
								  TotalChocolateBarWeight = f.Bars.OfType<ChocolateBar>().Sum(x => x.Weight),
								  TotalIronBarWeight = f.Bars.OfType<IronBar>().Sum(x => x.Weight)
							  };

				Reduce = results => from r in results
									group r by r.Tag into g
									select new
									{
										Tag = g.Key,
										Count = g.Sum(x => x.Count),
										TotalChocolateBarWeight = g.Sum(x => x.TotalChocolateBarWeight),
										TotalIronBarWeight = g.Sum(x => x.TotalIronBarWeight)
									};
			}
		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			
		}
	}
}