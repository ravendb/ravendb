using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class WhatChangedTests : RavenTest
    {
        [Fact]
        public void WhatChangeSupposeToWorkWithRavenJObject()
        {
            var obj = new { Id = (string)null, PropertyToRemove = true };
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(obj);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ravenObj = session.Load<RavenJObject>(obj.Id);

                    Assert.NotNull(ravenObj);

                    ravenObj.Remove("PropertyToRemove");
                    ravenObj.Add("PropertyToAdd", true);

                    Assert.False(ravenObj.ContainsKey("PropertyToRemove"));
                    Assert.True(ravenObj.ContainsKey("PropertyToAdd"));

                    //Not suppose to throw an exception
                    session.Advanced.WhatChanged();

                }
            }

        }
        [Fact]
        public void WhatChanged_Delete_After_Change_Value()
        {
            //RavenDB-13501
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    const string id = "ABC";
                    var o = new TestObject();
                    o.Id = id;
                    o.A = "A";
                    o.B = "A";
                    session.Store(o);
                    session.SaveChanges();
                    Assert.True(!session.Advanced.HasChanges);

                    o = session.Load<TestObject>(id);
                    o.A = "B"; 
                    o.B = "C"; 
                    session.Delete(o); 

                    var whatChanged = session.Advanced.WhatChanged();

                    Assert.True(whatChanged.Count == 1 
                                && whatChanged.Values.First()[0].Change == DocumentsChanges.ChangeType.DocumentDeleted);

                    session.SaveChanges();

                    o = session.Load<TestObject>(id);
                    Assert.True(o == null);
                }
            }
        }
        class TestObject
        {
            public string Id { get; set; }
            public string A { get; set; }
            public string B { get; set; }
        }
    }
}
