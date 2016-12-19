using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4729 : RavenTest
    {
        [Fact]
        public void CanSaveModifyEntityThatHasUntrackedProperties()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null, RavenJObject.Parse(UserDocumentAsStr), null);
                using (var session = store.OpenSession())
                {
                    var user = session.Load<TestUser>("users/1");
                    user.FirstName = "Bob";
                    session.SaveChanges();
                }
            }
        }
        public class TestUser
        {
            public string FirstName { get; set; }
        }
        private const string UserDocumentAsStr = "{\r\n  \"FirstName\": \"Nick\", \"LastName\": null\r\n}";
    }
}
