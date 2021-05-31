using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14164 : RavenTestBase
    {
        public RavenDB_14164(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGetTimeSeriesWithIncludeTagDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] {"watches/fitbit", "watches/apple", "watches/sony"};
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2), 
                            includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.855, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);

                }
            }
        }

        [Fact]
        public async Task CanGetTimeSeriesWithIncludeTagDocuments_Async()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), documentId);

                    await session.StoreAsync(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    await session.StoreAsync(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    await session.StoreAsync(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, tags[i % 3]);
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var getResults = await session.TimeSeriesFor(documentId, "HeartRate").GetAsync(baseline, baseline.AddHours(2),
                            includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = await session.LoadAsync<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.855, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesWithIncludeTagsAndParentDocument()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2),
                            includes: builder => builder
                                .IncludeTags()
                                .IncludeDocument());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var user = session.Load<User>(documentId);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal("ayende", user.Name);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.855, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);

                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesWithInclude_CacheNotEmpty()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        string tag;
                        if (i < 60)
                        {
                            tag = tags[0];
                        }
                        else if (i < 90)
                        {
                            tag = tags[1];
                        }
                        else
                        {
                            tag = tags[2];
                        }
                        tsf.Append(baseline.AddMinutes(i), i, tag);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // get [00:00 - 01:00]
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(1));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(1), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // get [01:15 - 02:00] with includes
                    getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline.AddMinutes(75), baseline.AddHours(2),
                        includes: builder => builder.IncludeTags());

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(46, getResults.Length);
                    Assert.Equal(baseline.AddMinutes(75), getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(new []{tags[1], tags[2]});
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(2, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);

                    // "watches/fitbit" should not be in cache

                    var watch = session.Load<Watch>(tags[0]);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal("FitBit", watch.Name);
                    Assert.Equal(0.855, watch.Accuracy);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesWithInclude_CacheNotEmpty2()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        string tag;
                        if (i < 60)
                        {
                            tag = tags[0];
                        }
                        else if (i < 90)
                        {
                            tag = tags[1];
                        }
                        else
                        {
                            tag = tags[2];
                        }
                        tsf.Append(baseline.AddMinutes(i), i, tag);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // get [00:00 - 01:00]
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(1));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(1), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // get [01:30 - 02:00]
                    getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline.AddMinutes(90), baseline.AddHours(2));

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(31, getResults.Length);
                    Assert.Equal(baseline.AddMinutes(90), getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // get [01:00 - 01:15] with includes
                    getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline.AddHours(1), baseline.AddMinutes(75), 
                        includes: builder => builder.IncludeTags());

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(16, getResults.Length);
                    Assert.Equal(baseline.AddHours(1), getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(75), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var watch = session.Load<Watch>(tags[1]);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal("Apple", watch.Name);
                    Assert.Equal(0.9, watch.Accuracy);

                    // tags[0] and tags[2] should not be in cache

                    watch = session.Load<Watch>(tags[0]);
                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal("FitBit", watch.Name);
                    Assert.Equal(0.855, watch.Accuracy);

                    watch = session.Load<Watch>(tags[2]);
                    Assert.Equal(5, session.Advanced.NumberOfRequests);
                    Assert.Equal("Sony", watch.Name);
                    Assert.Equal(0.78, watch.Accuracy);

                }
            }
        }

        [Fact]
        public void CanGetMultipleRangesWithIncludes()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // get range [00:00 - 00:30]
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddMinutes(30));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(31, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(30), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // get range [00:45 - 00:60]
                    getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline.AddMinutes(45), baseline.AddHours(1));

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(16, getResults.Length);
                    Assert.Equal(baseline.AddMinutes(45), getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(60), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // get range [01:30 - 02:00]
                    getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline.AddMinutes(90), baseline.AddHours(2));

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(31, getResults.Length);
                    Assert.Equal(baseline.AddMinutes(90), getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // get multiple ranges with includes

                    // ask for entire range [00:00 - 02:00] with includes
                    // this will go to server to get the "missing parts" - [00:30 - 00:45] and [01:00 - 01:30] 

                    getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2),
                        includes: builder => builder
                            .IncludeTags()
                            .IncludeDocument());

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var user = session.Load<User>(documentId);
                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                    Assert.Equal("ayende", user.Name);

                    // should not go to server

                    var tagDocs = session.Load<Watch>(tags);
                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagDocs.Count);
                    var tagDoc = tagDocs["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.855, tagDoc.Accuracy);

                    tagDoc = tagDocs["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagDocs["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);

                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesWithIncludeTags_WhenNotAllEntriesHaveTags()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        var tag = i % 10 == 0 
                            ? null 
                            : tags[i % 3];

                        tsf.Append(baseline.AddMinutes(i), i, tag);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2),
                            includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.855, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);

                }
            }
        }

        [Fact]
        public void IncludesShouldAffectTimeSeriesGetCommandEtag()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] { "watches/fitbit", "watches/apple", "watches/sony" };
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    session.Store(new Watch
                    {
                        Name = "FitBit",
                        Accuracy = 0.855
                    }, tags[0]);
                    session.Store(new Watch
                    {
                        Name = "Apple",
                        Accuracy = 0.9
                    }, tags[1]);
                    session.Store(new Watch
                    {
                        Name = "Sony",
                        Accuracy = 0.78
                    }, tags[2]);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i <= 120; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2),
                            includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.855, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/apple"];
                    Assert.Equal("Apple", tagDoc.Name);
                    Assert.Equal(0.9, tagDoc.Accuracy);

                    tagDoc = tagsDocuments["watches/sony"];
                    Assert.Equal("Sony", tagDoc.Name);
                    Assert.Equal(0.78, tagDoc.Accuracy);

                }

                using (var session = store.OpenSession())
                {
                    // update tags[0]

                    var watch = session.Load<Watch>(tags[0]);
                    watch.Accuracy += 0.05;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2),
                        includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert that tags[0] has updated Accuracy

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("FitBit", tagDoc.Name);
                    Assert.Equal(0.905, tagDoc.Accuracy);
                }

                var newTag = "watches/google";

                using (var session = store.OpenSession())
                {
                    // add new watch

                    session.Store(new Watch
                    {
                        Accuracy = 0.75,
                        Name = "Google Watch"
                    }, newTag);

                    // update a time series entry to have the new tag

                    session.TimeSeriesFor(documentId, "HeartRate")
                        .Append(baseline.AddMinutes(45), 90, tag: newTag);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline, baseline.AddHours(2),
                        includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server
                    session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert that newTag is in cache
                    var watch = session.Load<Watch>(newTag);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal("Google Watch", watch.Name);
                    Assert.Equal(0.75, watch.Accuracy);
                }
            }
        }


        private class Watch
        {
            public string Name { get; set; }

            public double Accuracy { get; set; }

        }
    }
}
