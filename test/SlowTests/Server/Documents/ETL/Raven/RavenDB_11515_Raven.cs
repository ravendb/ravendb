using System;
using FastTests.Server.JavaScript;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_11515_Raven : EtlTestBase
    {
        public RavenDB_11515_Raven(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Can_filter_out_deletions_of_documents(string jsEngineType)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, "Users", script:
@"
    loadToUsers(this);

    function deleteDocumentsOfUsersBehavior(docId) {
        return false;
    }
");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Can_define_multiple_delete_behavior_functions(string jsEngineType)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, collections: new []{ "Users", "Employees"}, script:
                    @"
    loadToUsers(this);

    function deleteDocumentsOfUsersBehavior(docId) {
        return false;
    }

function deleteDocumentsOfEmployeesBehavior(docId) {
        return false;
    }
");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        LastName = "Joe"
                    }, "users/1");

                    session.Store(new Employee()
                    {
                        LastName = "Joe"
                    }, "employees/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotEmpty(session.Advanced.LoadStartingWith<User>("employees/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotEmpty(session.Advanced.LoadStartingWith<User>("employees/1"));
                }
            }
        }
    }
}
