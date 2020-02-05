using System;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10493: RavenTestBase
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
                                    DateTime = x.DateTime,
                                    DateTimeMinValue = DateTime.MinValue,
                                    DateTimeMaxValue = DateTime.MaxValue
                                };

                    var expectedQuery =
                        $"declare function output(x) {{{Environment.NewLine}\tvar test = 1;{Environment.NewLine}\treturn {{ DateTime : x.DateTime, DateTimeMinValue : new Date(-62135596800000), DateTimeMaxValue : new Date(253402297199999) }};{Environment.NewLine}}}{Environment.NewLine}from 'Articles' as x select output(x)";

                    Assert.Equal(expectedQuery, query.ToString());

                    var result = query.ToList();
                    
                    Assert.Equal(DateTime.MinValue, result[0].DateTimeMinValue);

                    // Only missing 0.9999 ms, but with additional timezone
                    var epsilon = 1 + Math.Abs((DateTime.UtcNow - DateTime.Now).TotalSeconds); // Lower than 1 ms
                    var val = (DateTime.MaxValue - result[0].DateTimeMaxValue).TotalSeconds;
                    Assert.True(Math.Abs(val) < (epsilon));
                }                              
            }
        }
                        
    }
}
