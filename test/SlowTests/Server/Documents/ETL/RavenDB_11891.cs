using System;
using System.IO;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_11891 : EtlTestBase
    {
        [Fact]
        public void Should_filter_out_deletions_using_generic_delete_behavior()
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

                etlDone.Wait(TimeSpan.FromMinutes(1));

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

                etlDone.Wait(TimeSpan.FromSeconds(30));

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

                etlDone.Wait(TimeSpan.FromSeconds(30));

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

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.True(session.Advanced.Attachments.Exists("users/1", "photo"));

                    Assert.Null(session.Load<User>("employees/1"));
                }
            }
        }
        
        [Theory]
        [InlineData(new string[0], true)]
        [InlineData(new[] { "Users", "Employees" }, false)]
        public void Should_filter_out_deletions_using_generic_delete_behavior_function_and_marker_document(string[] collections, bool applyToAllDocuments)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
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

                etlDone.Wait(TimeSpan.FromMinutes(1));

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

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<User>("employees/1"));
                }
            }
        }

        [Fact]
        public void Should_filter_out_deletions_using_delete_behavior_function_and_marker_document()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new []{"Employees"}, script:
                    @"
function deleteDocumentsOfEmployeesBehavior(docId) {    
    var deletionCheck = load('DeletionLocalOnly/' + docId);
    if(deletionCheck){
        return false;
    }
    return true;
}

");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new Employee()
                    {
                        FirstName = "Doe"
                    }, "employees/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

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

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("employees/1"));
                }
            }
        }

        [Theory]
        [InlineData(@"
    loadToUsers(this);

    function deleteDocumentsBehavior(docId, collection) {
        
        return false;
    }

    function deleteDocumentsOfEmployeesBehavior(docId) {
        return true;
    }
")]
        [InlineData(@"
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
")]
        public void Should_choose_more_specific_delete_behavior_function(string script)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
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
                    Assert.Empty(session.Advanced.LoadStartingWith<User>("employees/1"));
                }
            }
        }
    }
}
