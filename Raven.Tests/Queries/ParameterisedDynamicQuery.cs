using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Tests.Storage;
using Xunit;
using Raven.Database;
using Raven.Database.Extensions;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Tests.Queries
{
    public class ParameterisedDynamicQuery : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

        public ParameterisedDynamicQuery()
        {
            db = new DocumentDatabase(new RavenConfiguration
                {
                    DataDirectory = "raven.db.test.esent",
                });
            db.SpinBackgroundWorkers();
        }

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

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

            db.Put("blogOne", null, JObject.FromObject(blogOne), new JObject(), null);
            db.Put("blogTwo", null, JObject.FromObject(blogTwo), new JObject(), null);
            db.Put("blogThree", null, JObject.FromObject(blogThree), new JObject(), null);

            var results = db.ExecuteDynamicQuery(new IndexQuery()
           {
               PageSize = 128,
               Start = 0,
               Cutoff = DateTime.Now,
               Query = "Title.Length:3 AND Category:Rhinos"
           });

            Assert.Equal(1, results.Results.Count);
            Assert.Equal("two", results.Results[0].Value<string>("Title"));
            Assert.Equal("Rhinos", results.Results[0].Value<string>("Category"));
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
}
