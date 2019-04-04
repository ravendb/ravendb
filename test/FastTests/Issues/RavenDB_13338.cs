using System;
using System.Dynamic;
using System.Linq;
using FastTests.Client;
using Microsoft.CSharp.RuntimeBinder;
using Newtonsoft.Json.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_13338 : RavenTestBase
    {
        [Fact]
        public void SkipAddingIdFieldToDynamicObjectShouldWork()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = a => a.Conventions.AddIdFieldToDynamicObjects = false
            }))
            {
                using (var session = store.OpenSession())
                {
                    dynamic o = new ExpandoObject();
                    o.Name = "Grisha";
                    session.Store(o);

                    var entity = JObject.Parse(@"{ User: 1 }");
                    session.Store(entity);
                    Assert.Equal(1, entity.Properties().Count());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadObject = session.Load<dynamic>("ExpandoObjects/1-A");
                    Assert.NotNull(loadObject);
                    Assert.Equal("Grisha", loadObject.Name.ToString());

                    var loadJObject = session.Load<dynamic>("JObjects/1-A");
                    Assert.NotNull(loadJObject);
                    Assert.Equal("1", loadJObject.User.ToString());
                    Assert.Null(loadJObject.Id);
                }
            }
        }
    }
}
