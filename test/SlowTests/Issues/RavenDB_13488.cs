using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13488:RavenTestBase
    {
        [Fact]
        public void ClearShouldClearDefferedCommandsAsWell()
        {
            using (var store = GetDocumentStore())
            {
                string firstCV = null;
                using (var session = store.OpenSession())
                {                    
                    User firstUser = new User
                    {
                        Name = "UserWithoutAttachment"
                    };
                    session.Store(firstUser, "users/1");
                    session.SaveChanges();
                    firstCV = session.Advanced.GetChangeVectorFor(firstUser);
                    firstUser.Age = 55;
                    session.SaveChanges();                    
                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    User concurrentUser = new User
                    {
                        Name = "UserWithAttachment"
                    };
                    session.Store(concurrentUser, firstCV, "users/1");
                    session.Advanced.Attachments.Store(concurrentUser, "myPic", new MemoryStream(new byte[] { 1, 2, 3, 4 }));

                    Assert.Throws<ConcurrencyException>(session.SaveChanges);
                    session.Advanced.Clear();                    
                    session.Store(concurrentUser, null, "users/1");

                    session.Query<User>().Lazily();
                    session.Advanced.Clear();
                    Assert.Equal(0, (session as InMemoryDocumentSessionOperations).DeferredCommandsCount);

                }
            }
        }        
    }
}
