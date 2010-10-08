using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Server;
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

        [Fact]
        public void SimpleQueriesDoNotGeneratedMultipleIndexes()
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

            int initialIndexCount = db.Statistics.CountOfIndexes;
            db.ExecuteDynamicQuery(new IndexQuery()
            {
                PageSize = 128,
                Start = 0,
                Cutoff = DateTime.Now,
                Query = "Title.Length:3 AND Category:Rhinos"
            });
            db.ExecuteDynamicQuery(new IndexQuery()
            {
                PageSize = 128,
                Start = 0,
                Cutoff = DateTime.Now,
                Query = "Title.Length:3 AND Category:Rhinos"
            });
            db.ExecuteDynamicQuery(new IndexQuery()
            {
                PageSize = 128,
                Start = 0,
                Cutoff = DateTime.Now,
                Query = "Category:Rhinos AND Title.Length:3"
            });

            Assert.True(db.Statistics.CountOfIndexes == initialIndexCount + 1);
                        
        }

        [Fact]
        public void SingleInvokedQueryShouldCreateOnlyCreatedTempIndex()
        {
            int initialIndexCount = db.Statistics.CountOfIndexes;

    
                db.ExecuteDynamicQuery(new IndexQuery()
                {
                    PageSize = 128,
                    Start = 0,
                    Cutoff = DateTime.Now,
                    Query = "Title.Length:3 AND Category:Rhinos"
                });
          

            var autoIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Auto_")).SingleOrDefault();
            var tempIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Temp_")).SingleOrDefault();

            Assert.False(string.IsNullOrEmpty(tempIndexName));
            Assert.True(string.IsNullOrEmpty(autoIndexName));
        }

        [Fact]
        public void LengthPropertySupportsRangedQueries()
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

            QueryResult results = null;
           
            results = db.ExecuteDynamicQuery(new IndexQuery()
                {
                    PageSize = 128,
                    Start = 0,
                    Cutoff = DateTime.Now,
                    Query = "Title.Length_Range:[0x00000004 TO 0x00000009]"
                });

            Assert.Equal(1, results.TotalResults);
        }
        [Fact]
        public void OftenInvokedQueryShouldCreatePermanentIndex()
        {
            int initialIndexCount = db.Statistics.CountOfIndexes;

            db.Configuration.TempIndexPromotionMinimumQueryCount = 2;
            db.Configuration.TempIndexPromotionThreshold = 2000;

            for (int x = 0; x < 3; x++)
            {
                db.ExecuteDynamicQuery(new IndexQuery()
                {
                    PageSize = 128,
                    Start = 0,
                    Cutoff = DateTime.Now,
                    Query = "Title.Length:3 AND Category:Rhinos"
                });
            }

            
            var autoIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Auto_")).SingleOrDefault();
            var tempIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Temp_")).SingleOrDefault();

            Assert.True(string.IsNullOrEmpty(tempIndexName));
            Assert.False(string.IsNullOrEmpty(autoIndexName));
        }

        [Fact]
        public void NestedCollectionPropertiesCanBeQueried()
        {
            var blogOne = new Blog
            {
                Title = "one",
                Category = "Ravens",
                Tags = new Tag[]{
                     new Tag(){ Name = "birds" }
                },
            };
            var blogTwo = new Blog
            {
                Title = "two",
                Category = "Rhinos",
                Tags = new Tag[]{
                     new Tag(){ Name = "mammals" }
                },
            };
            var blogThree = new Blog
            {
                Title = "three",
                Category = "Rhinos",
                Tags = new Tag[]{
                     new Tag(){ Name = "mammals" }
                },
            };

            db.Put("blogOne", null, JObject.FromObject(blogOne), new JObject(), null);
            db.Put("blogTwo", null, JObject.FromObject(blogTwo), new JObject(), null);
            db.Put("blogThree", null, JObject.FromObject(blogThree), new JObject(), null);

            var results = db.ExecuteDynamicQuery(new IndexQuery()
            {
                PageSize = 128,
                Start = 0,
                Cutoff = DateTime.Now,
                Query = "Tags,Name:[[birds]]"
            });

            Assert.Equal(1, results.Results.Count);
            Assert.Equal("one", results.Results[0].Value<string>("Title"));
            Assert.Equal("Ravens", results.Results[0].Value<string>("Category"));
        }
        
        [Fact]
        public void NestedPropertiesCanBeQueried()
        {
            var blogOne = new Blog
            {
                Title = "one",
                Category = "Ravens",
                User = new User(){ Name = "ayende" }
            };
            var blogTwo = new Blog
            {
                Title = "two",
                Category = "Rhinos",
                User = new User() { Name = "ayende" }
            };
            var blogThree = new Blog
            {
                Title = "three",
                Category = "Rhinos",
                User = new User() { Name = "rob" }
            };

            db.Put("blogOne", null, JObject.FromObject(blogOne), new JObject(), null);
            db.Put("blogTwo", null, JObject.FromObject(blogTwo), new JObject(), null);
            db.Put("blogThree", null, JObject.FromObject(blogThree), new JObject(), null);

            var results = db.ExecuteDynamicQuery(new IndexQuery()
            {
                PageSize = 128,
                Start = 0,
                Cutoff = DateTime.Now,
                Query = "User.Name:rob"
            });

            Assert.Equal(1, results.Results.Count);
            Assert.Equal("three", results.Results[0].Value<string>("Title"));
            Assert.Equal("Rhinos", results.Results[0].Value<string>("Category"));
        }

        public class Blog
        {
            public User User
            {
                get;
                set;
            }

            public string Title
            {
                get;
                set;
            }

            public Tag[] Tags
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

        public class Tag
        {
            public string Name
            {
                get;
                set;
            }
        }

        public class User
        {
            public string Name
            {
                get;
                set;
            }
        }
    }
}
