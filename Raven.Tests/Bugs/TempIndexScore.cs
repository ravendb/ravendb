using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class TempIndexScore : RavenTest
	{
		private class Blog
		{
			public string Title { get; set; }
			public string Category { get; set; }
			public BlogTag[] Tags { get; set; }
		}

		private class BlogTag
		{
			public string Name { get; set; }
		}

        private class SampleDataTransformer : AbstractTransformerCreationTask<Blog>
        {
            public SampleDataTransformer()
            {
                TransformResults = results => from result in results
                                              select new BlogScore
                                              {
                                                  Title = result.Title,
                                                  Score = MetadataFor(result).Value<float>("Temp-Index-Score")
                                              };
            }
        }

        private class BlogScore
        {
            public string Title { get; set; }
            public float Score { get; set; }
        }

	    private EmbeddableDocumentStore SetupSampleData()
	    {
            var blogOne = new Blog
            {
                Title = "one",
                Category = "Ravens",
                Tags = new[]
				{
					new BlogTag {Name = "Birds"}
				}
            };
            var blogTwo = new Blog
            {
                Title = "two",
                Category = "Rhinos",
                Tags = new[]
				{
					new BlogTag {Name = "Mammals"}
				}
            };
            var blogThree = new Blog
            {
                Title = "three",
                Category = "Rhinos",
                Tags = new[]
				{
					new BlogTag {Name = "Mammals"}
				}
            };

            var store = NewDocumentStore();
            using (var s = store.OpenSession())
            {
                s.Store(blogOne);
                s.Store(blogTwo);
                s.Store(blogThree);
                s.SaveChanges();
            }

            return store;
	    }

	    [Fact]
		public void ScoreShouldBeAValidFloatValue()
		{
            using (var store = SetupSampleData())
            {
				using (var session = store.OpenSession())
				{
					var result = session.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
						.First(x => x.Tags.Any(y => y.Name == "Birds"));

					var metadata = session.Advanced.GetMetadataFor(result);
					var score = metadata.Value<float>("Temp-Index-Score");
					Assert.True(score > 0f);
				}
			}
		}

        [Fact]
        public void ScoreShouldBeAValidDoubleValue()
        {
            using (var store = SetupSampleData())
            {
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Blog>()
                            .Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
                            .FirstOrDefault(x => x.Title == "one");

                    var metadata = session.Advanced.GetMetadataFor(result);
                    var score = metadata.Value<double>("Temp-Index-Score");
                    Assert.True(score > 0d);
                }
            }
        }

        [Fact]
        public void ScoreShouldBeAValidFloatValueInTransformer()
        {
            using (var store = SetupSampleData())
            {
                new SampleDataTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Blog>()
                            .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                            .TransformWith<SampleDataTransformer, BlogScore>()
                            .FirstOrDefault(x => x.Title == "one");

                    var score = result.Score;
                    Assert.True(score > 0f);
                }
            }
        }
	}
}