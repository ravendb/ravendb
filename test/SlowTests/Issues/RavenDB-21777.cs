using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21777 : RavenTestBase
{
    public RavenDB_21777(ITestOutputHelper output) : base(output)
    {
    }

    private const string ExpectedExceptionMessage = "Using multiple fields inside method 'Any' can lead to unexpected query results.";

    [RavenFact(RavenTestCategory.Querying)]
    public void TestEnumerableMethodCallAccessingMultipleFields()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var m1 = new Member() { UserId = "abc", Role = Role.Contributor, Value = 21 };
                var m2 = new Member() { UserId = "bcd", Role = Role.Contributor, Value = 37 };

                var e1 = new Entity() { Name = "CoolName", Members = new List<Member>() { m1, m2 } };
                
                session.Store(m1);
                session.Store(m2);
                session.Store(e1);
                
                var ed1 = new EntityWithDict() { SomeDict = new Dictionary<string, int>{ { "a", 2 }, { "b", 1 } } };
                
                session.Store(ed1);
                
                session.SaveChanges();
                
                var r1 = session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.UserId == "abc"))
                    .ToList();
                
                var r2 = session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.UserId == "abc") && x.Members.Any(y => y.Role == Role.Contributor) && x.Members.Any(y => y.Value == 37))
                    .ToList();
                
                var r3 = session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.Value > 20 || y.Value.Equals(37)))
                    .ToList();
                
                var r4 = session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.UserId.Equals("abc") || y.Value.Equals(37)))
                    .ToList();
                
                // We have to stick to this behavior
                var r5 = session.Query<EntityWithDict>()
                    .Where(x => x.SomeDict.Any(kvp => kvp.Key == "a" && kvp.Value == 1))
                    .ToList();
                
                Assert.NotEmpty(r1);
                Assert.NotEmpty(r2);
                Assert.NotEmpty(r3);
                Assert.NotEmpty(r4);
                Assert.NotEmpty(r5);
                
                var exception1 = Assert.Throws<InvalidOperationException>(() => session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.UserId == "abc" && y.Value == 37 || y.Role == Role.Contributor))
                    .ToList());

                Assert.Contains(ExpectedExceptionMessage, exception1.Message);
                
                var exception2 = Assert.Throws<InvalidOperationException>(() => session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.UserId.Equals("abc") && y.Value.Equals(37)))
                    .ToList());
                
                Assert.Contains(ExpectedExceptionMessage, exception2.Message);
                
                var exception5 = Assert.Throws<InvalidOperationException>(() => session.Query<Entity>()
                    .Where(x => x.Members.Any(y => y.Value > 20 && y.Value < 30 || y.Value.Equals(57) && y.UserId != "aaa"))
                    .ToList());
                
                Assert.Contains(ExpectedExceptionMessage, exception5.Message);
            }
        }
    }

    private class Entity
    {
        public string Name { get; set; }
        public List<Member> Members { get; set; }
    }

    private class Member
    {
        public string UserId { get; set; }
        public int Value { get; set; }
        public Role Role { get; set; }
    }

    private enum Role
    {
        Contributor
    }

    private class EntityWithDict
    {
        public Dictionary<string, int> SomeDict { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Entity, DummyIndex.Result>
    {
        public class Result
        {
            public string Name { get; set; }
            public string UserId { get; set; }
            public Role Role { get; set; }
            public int Value { get; set; }
        }
        
        public DummyIndex()
        {
            Map = entities => from entity in entities
                from member in entity.Members
                select new { entity.Name, member.UserId, member.Role, member.Value };
        }
    }
}
