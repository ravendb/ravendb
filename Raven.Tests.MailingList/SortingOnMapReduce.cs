// -----------------------------------------------------------------------
//  <copyright file="SortingOnMapReduce.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class SortingOnMapReduce : RavenTestBase
	{
		public class Tag
		{
			public string Name { get; set; }
		}

		public class TagWithCount
		{
			public string Name { get; set; }
			public long Count { get; set; }
		}

		[Fact]
		public void MapReduceSortingBug()
		{
			using (var ds = this.NewDocumentStore())
			{
				ds.DatabaseCommands.PutIndex("TagsCount",
					new IndexDefinition
					{
						Map = "from tag in docs.Tags select new { tag.Name, Count = 1 }",
						Reduce = "from result in results group " +
								 "result by result.Name into g " +
								 "select new { Name = g.Key, Count = g.Sum(x => x.Count) }",
					});

				using (var s = ds.OpenSession())
				{
					s.Store(new Tag { Name = "one" });
					s.Store(new Tag { Name = "two" });
					s.Store(new Tag { Name = "two" });
					s.Store(new Tag { Name = "three" });
					s.Store(new Tag { Name = "three" });
					s.Store(new Tag { Name = "three" });
					s.SaveChanges();

					var results =
						s.Query<TagWithCount>("TagsCount")
						 .Customize(c => c.WaitForNonStaleResults())
						 .OrderByDescending(x => x.Count)
						 .Take(1)
						 .ToList();

					Assert.Equal("three", results.Single().Name);
					Assert.Equal(3, results.Single().Count);
				}
			}
		}
	}
}
