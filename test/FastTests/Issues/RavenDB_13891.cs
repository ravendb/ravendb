﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_13891 : RavenTestBase
    {
        [Fact]
        public async Task PatchedDocumentShouldBeReplaced()
        {
            using (var store = GetDocumentStore())
            {
                string personId = null;
                using (var s = store.OpenAsyncSession())
                {
                    for (var i = 1; i < 3; i++)
                    {
                        var p = new Person
                        {
                            Modifications = new List<Person.UserChange>
                            {
                                new Person.UserChange
                                {
                                    Id = $"{i}",
                                    Timestamp = DateTime.Now
                                }
                            }
                        };
                        await s.StoreAsync(p);
                        personId = p.Id;
                    }

                    await s.SaveChangesAsync();

                    s.Advanced.Defer(
                        new PatchCommandData(
                            personId,
                            null,
                            new PatchRequest
                            {
                                Script = $@"this.Text = 'aaa';"
                            },
                            null));

                    var person = await s.LoadAsync<Person>(personId);
                    Assert.True(string.IsNullOrEmpty(person.Text));
                    Assert.Equal(1, person.Modifications.Count);
                    await s.SaveChangesAsync();

                    person = await s.LoadAsync<Person>(personId);
                    Assert.Equal("aaa", person.Text);
                    Assert.Equal(1, person.Modifications.Count);
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public string Text { get; set; }
            public List<UserChange> Modifications { get; set; }

            public class UserChange
            {
                public string Id { get; set; }
                public DateTime Timestamp { get; set; }
            }
        }
    }
}
