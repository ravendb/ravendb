using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class TempIndexScore : RavenTest
	{
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

		[Fact]
		public void ScoreShouldBeAValidFloatValue()
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

			using (var store = this.NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var result = s.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
						.Where(x => x.Tags.Any(y => y.Name == "Birds"))
						.First();

					var metadata = s.Advanced.GetMetadataFor(result);
					var score = metadata.Value<float>("Temp-Index-Score");
					Assert.True(score > 0f);
				}
			}
		}
	}
}
