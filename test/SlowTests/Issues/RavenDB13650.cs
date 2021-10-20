using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB13650 : RavenTestBase
    {
        public RavenDB13650(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
#pragma warning disable 649
            public Dictionary<string, string> Items;
#pragma warning restore 649
        }

        private class User_Index : AbstractIndexCreationTask<User>
        {
            public User_Index()
            {
                Map = users => from user in users
                               select new { Name = user.Items["Name"] };
            }
        }

        [Fact]
        public void SimpleDictionaryShouldBeSimple()
        {
            var indexDefinition = new User_Index().CreateIndexDefinition();
            Assert.Equal("docs.Users.Select(user => new {     Name = user.Items[\"Name\"] })",
                string.Join(" ", indexDefinition.Maps.First().Split(Environment.NewLine)));
        }
    }
}
