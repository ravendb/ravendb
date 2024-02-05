using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class WhatChangedTests : RavenTestBase
    {
        public WhatChangedTests(ITestOutputHelper output) : base(output)
        {
        }

        public class Entity
        {
            public Dictionary<string, string> SomeData { get; set; } = new();
        }

        // RavenDB-21749
        [RavenFact(RavenTestCategory.ClientApi)] 
        public async Task WhatChanged_RemovedFieldFromDictionary()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Entity { SomeData = { { "Key", "Value" } } }, "entities/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<Entity>("entities/1");

                entity.SomeData.Remove("Key");

                var changes = session.Advanced.WhatChanged()["entities/1"];

                Assert.Equal(1, changes.Length);
                Assert.Equal(DocumentsChanges.ChangeType.RemovedField, changes[0].Change);
                Assert.Equal("Key", changes[0].FieldName);
                Assert.Equal("Value", changes[0].FieldOldValue); // Note: This should not be null
                Assert.Equal(null, changes[0].FieldNewValue);
                Assert.Equal("SomeData", changes[0].FieldPath);
            }
        }
    }
}
