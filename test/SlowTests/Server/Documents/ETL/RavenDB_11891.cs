using System;
using System.IO;
using FastTests.Server.JavaScript;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_11891 : EtlTestBase
    {
        public RavenDB_11891(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_filter_out_deletions_using_delete_behavior_function_simple()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new[] { "Users" }, script:
                    @"
    function deleteDocumentsOfUsersBehavior(docId) {
        return false;
    }
");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);
                
                using (var session = src.OpenSession())
                {
                    var entity = new User()
                    {
                        Name = "Joe"
                    };
                    session.Store(entity, "users/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

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

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [Fact]
        public void Should_filter_out_deletions_using_generic_delete_behavior__simple()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: Array.Empty<string>(), script:
                    @"
    function deleteDocumentsBehavior(docId, collection) {
        return false;
    }
", applyToAllDocuments: true);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);
                
                using (var session = src.OpenSession())
                {
                    var entity = new User()
                    {
                        Name = "Joe"
                    };
                    session.Store(entity, "users/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

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

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [Fact]
        public void Should_filter_out_deletions_using_delete_behavior_function_and_marker_document__simple()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new[] { "Employees" }, script:
                    @"
function deleteDocumentsOfEmployeesBehavior(docId) {    
    return false;
}

");
                var last = 0;
                var etlDone = WaitForEtl(src, (n, s) =>
                {
                    var check = s.LoadSuccesses > last;
                    last = s.LoadSuccesses;
                    return check;
                });

                using (var session = src.OpenSession())
                {
                    session.Store(new Employee()
                    {
                        FirstName = "Doe"
                    }, "employees/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("employees/1");
                    session.SaveChanges();
                }

                Assert.False(etlDone.Wait(TimeSpan.FromSeconds(3)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }
            }
        }

        
        [Theory]
        [JavaScriptEngineClassData]
        public void Should_filter_out_deletions_using_generic_delete_behavior(string jsEngineType)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, collections: new string[0], script:
                    @"
    
    function deleteDocumentsBehavior(docId, collection) {
        return 'Users' != collection;
    }
", applyToAllDocuments: true);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    var entity = new User()
                    {
                        Name = "Joe"
                    };
                    session.Store(entity, "users/1");

                    session.Store(new Employee()
                    {
                        LastName = "Joe"
                    }, "employees/1");


                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Null(session.Load<Employee>("employees/1"));
                }
            }
        }

        [Fact]
        public void Should_filter_out_deletions_using_generic_delete_behavior__doc_with_extensions()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new string[0], script:
                    @"
    
    function deleteDocumentsBehavior(docId, collection) {
        return 'Users' != collection;
    }
", applyToAllDocuments: true);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    var entity = new User()
                    {
                        Name = "Joe"
                    };
                    session.Store(entity, "users/1");

                    session.Advanced.Attachments.Store(entity, "photo", new MemoryStream(new byte[] { 1, 2, 3 }));

                    session.CountersFor(entity).Increment("likes");

                    session.Store(new Employee()
                    {
                        LastName = "Joe"
                    }, "employees/1");


                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    Assert.NotNull(session.Load<Employee>("employees/1"));
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
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.True(session.Advanced.Attachments.Exists("users/1", "photo"));

                    Assert.Null(session.Load<Employee>("employees/1"));
                }
            }
        }

        [Theory]
        [InlineData(new string[0], true, "Jint")]
        [InlineData(new[] { "Users", "Employees" }, false, "Jint")]
        [InlineData(new string[0], true, "V8")]
        [InlineData(new[] { "Users", "Employees" }, false, "V8")]
        public void Should_filter_out_deletions_using_generic_delete_behavior_function_and_marker_document(string[] collections, bool applyToAllDocuments, string jsEngineType)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, collections: collections, script:
                    @"
function deleteDocumentsBehavior(docId, collection) {    
    var deletionCheck = load('DeletionLocalOnly/' + docId);
    if(deletionCheck){
        return false;
    }
    return true;
}
", applyToAllDocuments: applyToAllDocuments);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.Store(new Employee()
                    {
                        FirstName = "Doe"
                    }, "employees/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("employees/1");

                    session.Store(new object(), "DeletionLocalOnly/employees/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }
            }
        }

        [Fact]
        public void Should_filter_out_deletions_using_delete_behavior_function_and_marker_document()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new[] { "Employees" }, script:
                    @"
function deleteDocumentsOfEmployeesBehavior(docId) {    
    var deletionCheck = load('DeletionLocalOnly/' + docId);
    if(deletionCheck){
        return false;
    }
    return true;
}

");
                var last = 0;
                var etlDone = WaitForEtl(src, (n, s) =>
                {
                    var check = s.LoadSuccesses > last;
                    last = s.LoadSuccesses;
                    return check;
                });

                using (var session = src.OpenSession())
                {
                    session.Store(new Employee()
                    {
                        FirstName = "Doe"
                    }, "employees/1");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("employees/1");

                    session.Store(new object(), "DeletionLocalOnly/employees/1");

                    session.SaveChanges();
                }

                Assert.False(etlDone.Wait(TimeSpan.FromSeconds(3)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<Employee>("employees/1"));
                }
            }
        }

        
        const string Should_choose_more_specific_delete_behavior_function_Script1 = @"
    loadToUsers(this);

    function deleteDocumentsBehavior(docId, collection) {
        
        return false;
    }

    function deleteDocumentsOfEmployeesBehavior(docId) {
        return true;
    }
";

        private const string Should_choose_more_specific_delete_behavior_function_Script2 = @"
    loadToUsers(this);

    function deleteDocumentsBehavior(docId, collection) {
        
       if (true)
       {   
          return false;
       }

       return true;
    }

    function deleteDocumentsOfEmployeesBehavior(docId) {
        return true;
    }
";
        
        [Theory]
        [InlineData(Should_choose_more_specific_delete_behavior_function_Script1, "Jint")]
        [InlineData(Should_choose_more_specific_delete_behavior_function_Script2, "Jint")]
        [InlineData(Should_choose_more_specific_delete_behavior_function_Script1, "V8")]
        [InlineData(Should_choose_more_specific_delete_behavior_function_Script2, "V8")]
        public void Should_choose_more_specific_delete_behavior_function(string script, string jsEngineType)
        {
            var options = Options.ForJavaScriptEngine(jsEngineType);
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, collections: new[] { "Users", "Employees" }, script: script);

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

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

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

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Empty(session.Advanced.LoadStartingWith<User>("employees/1"));
                }
            }
        }
    }
}
