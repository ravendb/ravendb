using System;
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
                    
                    Assert.Equal($"declare function output(x) {{{Environment.NewLine}\tvar test = 1;{Environment.NewLine}\treturn {{ CheckGroup : (((x.Quantity!=null?x.Quantity:0))!==0?2:3)===2?1:0, CheckGroup1 : (x.Quantity==null?1:2)===1?1:2, CheckGroup2 : (x.Quantity!=null?x.Quantity:0), CheckGroup3 : (x.Quantity!=null?x.Quantity:0), CheckGroup4 : ((x.Quantity!=null?x.Quantity:0))!==0?2:3, CheckGroup5 : x.Quantity!=null?x.Quantity:0 }};{Environment.NewLine}}}{Environment.NewLine}from Articles as x select output(x)", query.ToString());

                    var result = query.ToList();
                    
                    Assert.Equal(1, result.Count);
                    Assert.Equal(0, result[0].CheckGroup);
                    Assert.Equal(1, result[0].CheckGroup1);
                    Assert.Equal(0, result[0].CheckGroup2);
                    Assert.Equal(0, result[0].CheckGroup3);
                    Assert.Equal(3, result[0].CheckGroup4);
                    Assert.Equal(0, result[0].CheckGroup5);
                }
            }
        }
    }
}
