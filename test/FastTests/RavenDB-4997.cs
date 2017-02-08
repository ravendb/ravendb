using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Document;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_4997 : ReplicationTestsBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public void Load_of_conflicted_document_with_tombstone_should_result_in_error()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.Store(new User { Name = "bar-foo" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                SetupReplication(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                using (var session = storeA.OpenSession())
                {

                    var e = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    Assert.Equal("users/1", e.DocId);
                    Assert.Equal(2, e.Conflicts.Count());

                    session.Load<User>("users/2"); //this should not throw
                }
            }
        }

        [Fact]
        public void Load_of_conflicted_document_with_another_document_should_result_in_error()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.Store(new User { Name = "bar-foo" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                SetupReplication(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                using (var session = storeA.OpenSession())
                {

                    var e = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    Assert.Equal("users/1", e.DocId);
                    Assert.Equal(2, e.Conflicts.Count());

                    session.Load<User>("users/2"); //this should not throw
                }
            }
        }

        [Fact]
        public void Delete_of_conflicted_document_should_resolve_conflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.Store(new User { Name = "bar-foo" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                SetupReplication(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                using (var session = storeA.OpenSession())
                {
                    session.Delete("users/1"); 
                    session.SaveChanges();
                }

                var conflicts = GetConflicts(storeA, "users/1");
                Assert.Equal(0,conflicts.Count);
            }
        }

        [Fact]
        public void Load_of_several_conflicted_document_should_result_in_error()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = storeC.OpenSession())
                {
                    session.Store(new User { Name = "foo-bar" }, "users/1");
                    session.SaveChanges();
                }

                SetupReplication(storeB, storeA);
                SetupReplication(storeC, storeA);

                WaitUntilHasConflict(storeA, "users/1", 3);

                using (var session = storeA.OpenSession())
                {
                    var e = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    Assert.Equal("users/1", e.DocId);
                    Assert.Equal(3, e.Conflicts.Count());
                }
            }
        }
    }
}