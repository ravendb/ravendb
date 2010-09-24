using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

using Raven.Bundles.DynamicQueries.Database;

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
                FieldMap = "Title:title,Category:category,Title.Length:titleLength",
                PageSize = 128,
                Start = 0,
                Query = "titleLength:3 AND category:Rhinos"
            });

            Assert.Equal(1, results.Results.Length);
            Assert.Equal("two", results.Results[0].Value<string>("Title"));
            Assert.Equal("Rhinos", results.Results[0].Value<string>("Category"));
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
