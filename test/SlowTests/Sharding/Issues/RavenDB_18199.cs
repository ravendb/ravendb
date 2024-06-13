using System.Net;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Commands;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_18199 : RavenTestBase
    {
        public RavenDB_18199(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldReturnNotModifiedWhenGettingDocsStartingWith(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var command = new GetDocumentsCommand(store.Conventions, "users", null, null, null, 0, 100, false);

                    session.Advanced.RequestExecutor.Execute(command, context);

                    var command2 = new GetDocumentsCommand(store.Conventions, "users", null, null, null, 0, 100, false);

                    session.Advanced.RequestExecutor.Execute(command2, context);

                    Assert.Equal(HttpStatusCode.NotModified, command2.StatusCode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldReturnNotModifiedWhenGettingDocsById(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var command = new GetDocumentsCommand(store.Conventions, new[] { "users/1", "users/2" }, null, null, null, null, false);

                    session.Advanced.RequestExecutor.Execute(command, context);

                    var command2 = new GetDocumentsCommand(store.Conventions, new[] { "users/1", "users/2" }, null, null, null, null, false);

                    session.Advanced.RequestExecutor.Execute(command2, context);

                    Assert.Equal(HttpStatusCode.NotModified, command2.StatusCode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldGenerateCSharpClass(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var command = new GenerateClassFromDocumentCommand("users/1", "csharp");

                    session.Advanced.RequestExecutor.Execute(command, context);

                    string commandResult = command.Result;

                    RavenTestHelper.AssertEqualRespectingNewLines("""
                                                                  using System;
                                                                  using System.Collections.Generic;
                                                                  using System.Linq;
                                                                  using System.Text;
                                                                  using System.Threading.Tasks;

                                                                  namespace SlowTests.Core.Utils.Entities
                                                                  {
                                                                      public class User
                                                                      {
                                                                          public object AddressId { get; set; } 
                                                                          public int Count { get; set; } 
                                                                          public object LastName { get; set; } 
                                                                          public object Name { get; set; } 
                                                                      }
                                                                  }
                                                                  """, commandResult);
                }
            }
        }
    }
}
