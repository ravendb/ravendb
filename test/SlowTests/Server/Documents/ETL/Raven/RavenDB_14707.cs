using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Xunit.Abstractions;
using Raven.Tests.Core.Utils.Entities;
using System.IO;
using FastTests.Server.JavaScript;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_14707 : EtlTestBase
    {
        public RavenDB_14707(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Should_delete_existing_document_when_filtered_by_script(string jsEngineType)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, "Users", script: @"if (this.Name == 'Joe Doe') loadToUsers(this);");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.Name = "John Doe";
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
    }
}
