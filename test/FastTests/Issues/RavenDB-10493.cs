using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10493: RavenTestBase
    {        
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

                    Assert.Equal("declare function output(x) {\r\n\tvar test = 1;\r\n\treturn { DateTime : x.DateTime, DateTimeMinValue : new Date(-62135596800000), DateTimeMaxValue : new Date(253402297199999) };\r\n}\r\nfrom Articles as x select output(x)", query.ToString());

                    var result = query.ToList();
                    
                    Assert.Equal(DateTime.MinValue, result[0].DateTimeMinValue);

                    // Only missing 0.9999 ms
                    var epsilon = 1; // Lower than 1 ms
                    Assert.True(
                        Math.Abs((DateTime.MaxValue.ToUniversalTime() - result[0].DateTimeMaxValue).TotalSeconds) < epsilon);

                }                              
            }
        }
                        
    }
}
