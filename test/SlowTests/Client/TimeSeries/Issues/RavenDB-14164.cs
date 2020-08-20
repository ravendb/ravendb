using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
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
        public void CanIncludeTagDocumentsDuringTimeSeriesLoading()
        {
            using (var store = GetDocumentStore())
            {
                var tags = new[] {"watches/fitbit", "watches/apple", "watches/sony"};
                var baseline = DateTime.Today;

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
                    var tsf = session.TimeSeriesFor(documentId, "Heartrate");

                    for (int i = 0; i < 120; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var getResults = session.TimeSeriesFor(documentId, "HeatRate").Get(baseline, baseline.AddHours(2), 
                            includes: builder => builder.IncludeTags());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(120, getResults.Length);
                    Assert.Equal(baseline, getResults[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), getResults[^1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    // should not go to server

                    var tagsDocuments = session.Load<Watch>(tags);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // assert tag documents

                    Assert.Equal(3, tagsDocuments.Count);

                    var tagDoc = tagsDocuments["watches/fitbit"];
                    Assert.Equal("Fitbit", tagDoc.Name);
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

        private class Watch
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public double Accuracy { get; set; }

        }
    }
}
