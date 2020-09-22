using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Operations.ETL;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_11897 : EtlTestBase
    {
        public RavenDB_11897(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(@"
    
    function deleteDocumentsOfUsersBehavior(docId) {
        return false;
    }
")]
        [InlineData(@"
    
    function deleteDocumentsOfUsersBehavior(docId) {
      if (true)
      {
         return false;
      }

      return true;
    }
")]
        public void Should_handle_as_empty_script_but_filter_out_deletions(string script)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: script);

                var last = 0;
                var etlDone = WaitForEtl(src, (n, s) =>
                {
                    var check = s.LoadSuccesses > last;
                    last = s.LoadSuccesses;
                    return check;
                });

                using (var session = src.OpenSession())
                {
                    var entity = new User()
                    {
                        Name = "Joe"
                    };
                    session.Store(entity, "users/1");

                    session.Advanced.Attachments.Store(entity, "photo", new MemoryStream(new byte[] { 1, 2, 3 }));

                    session.CountersFor(entity).Increment("likes");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.True(session.Advanced.Attachments.Exists("users/1", "photo"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "photo");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.True(session.Advanced.Attachments.Exists("users/1", "photo"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Delete("likes");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(1, session.CountersFor("users/1").Get("likes"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                Assert.False(etlDone.Wait(TimeSpan.FromSeconds(3)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.True(session.Advanced.Attachments.Exists("users/1", "photo"));
                }
            }
        }

        [Fact]
        public void Error_if_script_does_not_contain_any_loadTo_method_and_isnt_empty()
        {
            var config = new RavenEtlConfiguration
            {
                Name = "test",
                ConnectionStringName = "test",
                Transforms =
                {
                    new Transformation
                    {
                        Name = "test",
                        Collections = {"Users"},
                        Script = @"
                        
                        this.Name = 'aaa'; // we have some modification but we don't call loadTo method so it should error

                        function deleteDocumentsOfUsersBehavior(docId) {
                            return false;
                        }
                    "
                    }
                }
            };

            config.Initialize(new RavenConnectionString() { Database = "Foo", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } });

            List<string> errors;
            config.Validate(out errors);

            Assert.Equal(1, errors.Count);

            Assert.Equal("No `loadTo<CollectionName>()` method call found in 'test' script", errors[0]);
        }
    }
}
