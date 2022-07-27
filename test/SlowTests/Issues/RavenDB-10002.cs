using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10002: RavenTestBase
    {        
        public RavenDB_10002(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public List<string> Properties;
        }
                        
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanHaveArrayInMetadata(Options options)
        {             
            using (var store = GetDocumentStore(options))
            {             
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        Properties = new List<string>{"Name", "Date"}
                    });
                    session.Store(new Article
                    {
                        Properties = new List<string>()
                    });
                    session.SaveChanges();
                }
                                
                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                select new
                                {
                                    HasProperties = x.Properties.Any()
                                }; 
                    
                    Assert.Equal("from 'Articles' as x select { HasProperties : (x?.Properties?.length??0) > 0 }", query.ToString());

                    var result = query.ToList();
                    
                    Assert.Equal(2, result.Count);
                    Assert.True(result[0].HasProperties);
                    Assert.False(result[1].HasProperties);

                }                              
            }
        }
                        
    }
}
