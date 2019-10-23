using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8917 : RavenTestBase
    {
        public RavenDB_8917(ITestOutputHelper output) : base(output)
        {
        }

        private static string DocId = "test";

        [Fact]
        public async Task can_change_from_null_to_array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new ApplicationState
                    {
                        Name = "Grisha",
                        Files = new ApplicationFiles
                        {
                            Excluded = null,
                            Ignored = new[] { "aa", "bb" }
                        }
                    }, DocId);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var application = await session.LoadAsync<ApplicationState>(DocId);
                    application.Files = new ApplicationFiles
                    {
                        Excluded = new[] { "1", "2" },
                        Ignored = new[] { "aa", "bb" }
                    };

                    var changes = session.Advanced.WhatChanged();
                    Assert.Equal(1, changes.Count);
                    Assert.Equal(1, changes[DocId].Length);

                    Assert.Equal("Excluded", changes[DocId][0].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes[DocId][0].Change);
                    Assert.Equal(null, changes[DocId][0].FieldOldValue);
                    Assert.Equal("[\"1\",\"2\"]", changes[DocId][0].FieldNewValue.ToString());

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task can_change_from_array_to_null()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new ApplicationState
                    {
                        Name = "Grisha",
                        Files = new ApplicationFiles
                        {
                            Excluded = new[] { "aa", "bb" },
                            Ignored = new[] { "aa", "bb" }
                        }
                    }, DocId);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var application = await session.LoadAsync<ApplicationState>(DocId);
                    application.Files = new ApplicationFiles
                    {
                        Excluded = null,
                        Ignored = new[] { "1", "2" }
                    };

                    await session.SaveChangesAsync();
                }
            }
        }

        public sealed class ApplicationState
        {
            public ApplicationFiles Files { get; set; }
            public string Name { get; set; }
        }

        public sealed class ApplicationFiles
        {
            public string[] Excluded { get; set; }
            public string[] Ignored { get; set; }
        }
    }
}
