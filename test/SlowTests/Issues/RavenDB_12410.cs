using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12410 : RavenTestBase
    {
        private string _docId = "users/1-A";

        private char c = 'a';

        private class User
        {
            public DateTime LastLogin { get; set; }
            public Name1 Name1 { get; set; }
            public string StamString { get; set; }
            public int StamInteger { get; set; }
        }

        private class Name1
        {
            public Name2 Name2 { get; set; }
        }

        private class Name2
        {
            public Name3 Name3 { get; set; }
        }
        private class Name3
        {
            public Name4 Name4 { get; set; }
        }
        private class Name4
        {
            public string MyNestedObjectText { get; set; }
        }
        [Fact]
        public void Nested_Documents_Get_Patched()
        {
            var now = DateTime.Now;

            var user = new User
            {
                LastLogin = now,
                Name1 = new Name1
                {
                    Name2 = new Name2
                    {
                        Name3 = new Name3
                        {
                            Name4 = new Name4
                            {
                                MyNestedObjectText = $"{c}"
                            }
                        }
                    }
                },
                StamString = "JustString",
                StamInteger = 322
            };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }
                for (int i = 0; i < 20; i++)
                {
                    string currentStr;

                    using (var session = store.OpenSession())
                    {
                        var loaded = session.Load<User>(_docId);
                        currentStr = loaded.Name1.Name2.Name3.Name4.MyNestedObjectText;

                        Assert.Equal(currentStr, loaded.Name1.Name2.Name3.Name4.MyNestedObjectText);
                    }

                    c++;
                    var nextStr = currentStr + c;

                    using (var session = store.OpenSession())
                    {
                        var operation = store
                            .Operations
                            .Send(new PatchByQueryOperation("from Users as u update { " +
                                                            $"u.Name1.Name2.Name3.Name4.MyNestedObjectText = \"{nextStr}\"" +
                                                            "; }"));

                        operation.WaitForCompletion(TimeSpan.FromMinutes(1));
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var loaded = session.Load<User>(_docId);
                        Assert.Equal(loaded.LastLogin, now);
                        Assert.Equal(loaded.Name1.Name2.Name3.Name4.MyNestedObjectText, nextStr);
                        Assert.Equal(loaded.StamString, "JustString");
                        Assert.Equal(loaded.StamInteger, 322);
                    }
                }
            }
        }
    }
}
