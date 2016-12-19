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
    }
}
