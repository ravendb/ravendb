//-----------------------------------------------------------------------
// <copyright file="DocumentUrl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Xunit;

namespace SlowTests.Bugs
{
    public class DocumentUrl : RavenTestBase
    {
        [Fact]
        public void CanGetFullUrl_WithSlashOnTheEnd()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var entity = new User();
                    session.Store(entity);

                    var storedUrl = session.Advanced.GetDocumentUrl(entity);
                    var serverUrl = documentStore.Urls;
                    
                    Assert.Equal(serverUrl + "/databases/"+documentStore.Database+ "/docs?id=users/1-A", storedUrl);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Location { get; set; }
            public int Age { get; set; }

        }
    }
}
