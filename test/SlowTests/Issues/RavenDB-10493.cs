using System;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10493 : RavenTestBase
    {
        public RavenDB_10493(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public DateTime? DateTime;
        }

        [Fact]
        public void CanTranslateDateTimeMinValueMaxValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        DateTime = null
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                let test = 1
                                select new
                                {
                                    x.DateTime,
                                    DateTimeMinValue = DateTime.MinValue,
                                    DateTimeMaxValue = DateTime.MaxValue,
                                    DateTimeUtcNow = DateTime.UtcNow,
                                    DateTimeToday = DateTime.Today
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(@"declare function output(x) {
	var test = 1;
	return { DateTime : x.DateTime, DateTimeMinValue : new Date(-62135596800000), DateTimeMaxValue : new Date(253402297199999), DateTimeUtcNow : ((date) => new Date(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds(), date.getUTCMilliseconds()))(new Date()), DateTimeToday : new Date(new Date().setHours(0,0,0,0)) };
}
from 'Articles' as x select output(x)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(DateTime.MinValue, result[0].DateTimeMinValue);

                    Assert.Equal(DateTime.UtcNow, result[0].DateTimeUtcNow, TimeSpan.FromSeconds(20));
                    
                    // We want to have one day margin, so if we assign DateTimeToday before midnight, and check the assertion
                    // after, the test will not fail
                    Assert.Equal(DateTime.Today, result[0].DateTimeToday, TimeSpan.FromDays(1));

                    // Only missing 0.9999 ms, but with additional timezone
                    var epsilon = 1 + Math.Abs((DateTime.UtcNow - DateTime.Now).TotalSeconds); // Lower than 1 ms
                    var val = (DateTime.MaxValue - result[0].DateTimeMaxValue).TotalSeconds;
                    Assert.True(Math.Abs(val) < epsilon, $"Math.Abs({val}) < ({epsilon})");
                }
            }
        }
    }
}
