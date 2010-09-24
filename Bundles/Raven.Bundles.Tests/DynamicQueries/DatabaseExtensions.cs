using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

using Raven.Bundles.DynamicQueries.Database;
using Raven.Bundles.DynamicQueries.Data;

namespace Raven.Bundles.Tests.DynamicQueries
{
    public class DatabaseExtensions : DynamicQueriesBase
    {
        [Fact]
        public void CanPerformDynamicQueryAndGetValidResults()
        {
            var blogOne = new Blog
			{
				Title = "one",
                Category = "Ravens"
			};
            var blogTwo = new Blog
			{
				 Title = "two",
                 Category = "Rhinos"
			};
            var blogThree = new Blog
			{
                Title = "three",
                Category = "Rhinos"
			};

            using (var s = store.OpenSession())
            {
                s.Store(blogOne);
                s.Store(blogTwo);
                s.Store(blogThree);
                s.SaveChanges();
            }

            var results = server.Database.ExecuteDynamicQuery(new Bundles.DynamicQueries.Data.DynamicQuery()
            {
                PageSize = 128,
                Start = 0,
                CutOff= DateTime.Now,
                Query = "Title.Length:3 AND Category:Rhinos"
            });

            Assert.Equal(1, results.Results.Length);
            Assert.Equal("two", results.Results[0].Value<string>("Title"));
            Assert.Equal("Rhinos", results.Results[0].Value<string>("Category"));
        }

        [Fact]
        public void CanPerformMultipleQueriesWithSameParametersAndOnlyCreateASingleIndex()
        {
            var blogOne = new Blog
			{
				Title = "one",
                Category = "Ravens"
			};
            var blogTwo = new Blog
			{
				 Title = "two",
                 Category = "Rhinos"
			};
            var blogThree = new Blog
			{
                Title = "three",
                Category = "Rhinos"
			};

            using (var s = store.OpenSession())
            {
                s.Store(blogOne);
                s.Store(blogTwo);
                s.Store(blogThree);
                s.SaveChanges();
            }

            int originalIndexCount = server.Database.Statistics.CountOfIndexes;

            server.Database.ExecuteDynamicQuery(new Bundles.DynamicQueries.Data.DynamicQuery()
            {
                PageSize = 128,
                Start = 0,
                Query = "Title.Length:3 AND Category:Rhinos"
            });
            server.Database.ExecuteDynamicQuery(new Bundles.DynamicQueries.Data.DynamicQuery()
            {
                PageSize = 128,
                Start = 0,
                Query = "Title.Length:3 AND Category:Rhinos"
            });
            server.Database.ExecuteDynamicQuery(new Bundles.DynamicQueries.Data.DynamicQuery()
            {
                PageSize = 128,
                Start = 0,
                Query = "Category:Rhinos AND Title.Length:3"
            });

            int secondIndexCount = server.Database.Statistics.CountOfIndexes;

            // Should only have created a single index
            Assert.True(secondIndexCount == originalIndexCount + 1);
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
    }
}
