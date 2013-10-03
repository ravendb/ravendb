//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithLocalServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Querying
{
	public class CollectionBasedAutoIndexes : RavenTest
	{
		[Fact]
		public void CanPerformDynamicQueryUsingClientLinqQueryWithNestedCollection()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens",
				 Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Birds" }
				 }
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos",
				Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Mammals" }
				 }
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos",
				Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Mammals" }
				 }
			};

			using(var store = this.NewDocumentStore())
			{               
				using (var s = store.OpenSession())
				{
				    RavenQueryStatistics stats;
				    var results = s.Query<Blog>()
            .Statistics(out stats)
						.Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
						.Where(x => x.Tags.Any(y=>y.Name == "Birds"))
						.ToArray();

          var index = store.DocumentDatabase.GetIndexDefinition(stats.IndexName);


				}
			}
		}

	   
		public class Blog
		{
			public string Title
			{
				get;
				set;
			}

			public string Category
			{
				get;
				set;
			}

			public BlogTag[] Tags
			{
				get;
				set;
			}
		}

		public class BlogTag
		{
			public string Name { get; set; }
		}
	}
}
