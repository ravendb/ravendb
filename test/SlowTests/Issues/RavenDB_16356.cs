using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16356 : RavenTestBase
    {
        public RavenDB_16356(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseRightConverterForIDictionaryWithDateTimeKey()
        {
            using (DocumentStore store = GetDocumentStore())
            {
                DateTime vwDay = Convert.ToDateTime("2019-11-01");
                using (var session = store.OpenSession())
                {
                    SocioDemographicData homeData = new SocioDemographicData();
                    homeData.Answers = new Dictionary<string, string>();
                    homeData.Answers.TryAdd("1", "A");
                    Home newHome = new Home() {Id = "322"};
                    newHome.Data.TryAdd(vwDay, homeData);
                    session.Store(newHome, "home/" + newHome.Id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var existingHome = session.Load<Home>("home/" + "322");
                    Assert.Equal(vwDay, existingHome.Data.First().Key);
                    Assert.Equal("1", existingHome.Data.First().Value.Answers.First().Key);
                    Assert.Equal("A", existingHome.Data.First().Value.Answers.First().Value);
                }
            }
        }

        private class SocioDemographicData
        {
            public double Weight { get; set; }
            public IDictionary<string, string> Answers { get; set; }
        }

        private class Member
        {
            public Member()
            {
                Data = new Dictionary<DateTime, SocioDemographicData>();
            }

            public string Id { get; set; }

            public string HomeId { get; set; }

            public IDictionary<DateTime, SocioDemographicData> Data { get; set; }
        }

        private class Home
        {
            public Home()
            {
                Data = new Dictionary<DateTime, SocioDemographicData>();
                Members = new List<Member>();
            }

            public string Id { get; set; }

            public IDictionary<DateTime, SocioDemographicData> Data { get; set; }

            public IList<Member> Members { get; set; }
        }
    }
}