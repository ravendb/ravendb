using System;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_13906 : RavenTestBase
    {
        public RavenDB_13906(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OnAfterSaveChangesOnPatchShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var condition = false;
                string id;
                using (var session = store.OpenSession())
                {
                    var entity = new User
                    {
                        Name = "Egor",
                        Status = Status.Bad
                    };

                    session.Store(entity);
                    session.SaveChanges();
                    id = session.Advanced.GetDocumentId(entity);
                }

                var expected = Status.Good;
                using (var session = store.OpenSession())
                {
                    session.Advanced.OnAfterSaveChanges += (_, __) => { condition = true; };
                    var user = session.Load<User>(id);
                    session.Advanced.Patch(user, x => x.Status, expected);
                    session.SaveChanges();

                    Assert.True(condition);
                    Assert.Equal(expected, session.Load<User>(id).Status);
                }

                condition = false;

                using (var session = store.OpenSession())
                {
                    session.Advanced.OnAfterSaveChanges += (_, __) => { condition = true; };
                    var operation = store.Operations.Send(new PatchByQueryOperation("from Users update { this.Name = this.Name + '_J'; }"));
                    operation.WaitForCompletion(TimeSpan.FromMinutes(5));
                    session.SaveChanges();

                    Assert.False(condition);
                    Assert.Equal("Egor_J", session.Load<User>(id).Name);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public Status Status { get; set; }
        }

        public enum Status
        {
            None,
            Good,
            Bad
        }
    }
}
