using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10625: RavenTestBase
    {        
        private class Article
        {
            public int? Quantity { get; set; }
        }
                        
        [Fact]
        public void CanTranslateGroupsCorrectly()
        {             
            using (var store = GetDocumentStore())
            {             
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        
                    });
                    session.SaveChanges();
                }
                                
                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                let test = 1
                                select new
                                {
                                    CheckGroup = ((x.Quantity ?? 0) != 0 ? 2 : 3) == 2 ? 1 : 0,
                                    CheckGroup1 = (x.Quantity == null ? 1 : 2) == 1 ? 1 : 2,
                                    CheckGroup2 = (x.Quantity ?? 0),
                                    CheckGroup3 = x.Quantity ?? 0,
                                    CheckGroup4 = ((x.Quantity ?? 0)) != 0 ? 2 : 3,
                                    CheckGroup5 = x.Quantity != null ? x.Quantity : 0,
                                }; 
                    
                    //Assert.Equal("from Articles as x select { HasProperties : x.Properties.length > 0 }", query.ToString());

                    var result = query.ToList();
                    
                    Assert.Equal(1, result.Count);

                }                              
            }
        }
                        
    }
}
