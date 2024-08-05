using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17872 : ClusterTestBase
    {
        public RavenDB_17872(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CompareExchangeMetadata_Create_WithoutPropChange()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";
            const string metadataPropName = "RandomProp";
            const string metadataValue = "RandomValue";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata[metadataPropName] = metadataValue;
                await session.SaveChangesAsync();
            }

            //The session doesn't set the metadata but it can be seen in the studio. 
            // WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.Contains(metadataPropName, entity.Metadata.Keys);
                Assert.Equal(metadataValue, entity.Metadata[metadataPropName]);
            }
        }

        [Fact]
        public async Task CompareExchangeMetadata_CreateAndTryToChangeToSameVal()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";
            const string metadataPropName = "RandomProp";
            const string metadataValue = "RandomValue";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                entity.Metadata[metadataPropName] = metadataValue;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                entity.Metadata[metadataPropName] = metadataValue;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.Contains(metadataPropName, entity.Metadata.Keys);
                Assert.Equal(metadataValue, entity.Metadata[metadataPropName]);
            }
        }

        [Fact]
        public async Task CompareExchangeMetadata_CreateAndTryToChangeToDifferentVal()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";
            const string metadataPropName = "RandomProp";
            const string metadataValue = "RandomValue";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata[metadataPropName] = metadataValue;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                entity.Metadata[metadataPropName] = metadataValue+"1";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
             {
                 var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                 Assert.Contains(metadataPropName, entity.Metadata.Keys);
                 Assert.Equal(metadataValue+"1", entity.Metadata[metadataPropName]);
             }
        }

        [Fact]
        public async Task CompareExchangeMetadata_DontDoAnything()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                entity.Value.Prop = "Changed";
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.True(entity.Metadata == null || entity.Metadata.Count == 0, $"There's a metatada for the compare-exchange \"{id}\", when it shouldn't have a metadata!");
                Assert.Contains("Changed", entity.Value.Prop);
            }
        }

        [Fact]
        public async Task CompareExchangeMetadata_CreateAndTryToRemoveAndAddVal()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";
            const string metadataPropName = "RandomProp";
            const string metadataValue = "RandomValue";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata[metadataPropName] = metadataValue;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.True(entity.Metadata.Remove(metadataPropName));
                entity.Metadata[metadataPropName+"1"] = metadataValue + "1";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.Equal(1, entity.Metadata.Count);
                Assert.Contains(metadataPropName+ "1", entity.Metadata.Keys);
                Assert.Equal(metadataValue + "1", entity.Metadata[metadataPropName + "1"]);
            }
        }

        [Fact]
        public async Task CompareExchangeMetadata_CreateAndTryToRemove()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";
            const string metadataPropName = "RandomProp";
            const string metadataValue = "RandomValue";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata[metadataPropName] = metadataValue;
                entity.Metadata[metadataPropName + "1"] = metadataValue + "1";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.True(entity.Metadata.Remove(metadataPropName));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.Equal(1, entity.Metadata.Count);
                Assert.Contains(metadataPropName + "1", entity.Metadata.Keys);
                Assert.Equal(metadataValue + "1", entity.Metadata[metadataPropName + "1"]);
            }
        }

        
        private class AtomicGuard
        {
#pragma warning disable CS0649
            public string Id;
#pragma warning restore CS0649
        }

        [Fact(Skip = "This test will cause the atomic guard not be in sync with the doc")]
        public async Task Can_Modify_Empty_Metadata()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/1";

                using (var session = store.OpenAsyncSession(new SessionOptions() { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new TestObj(), id);
                    await session.SaveChangesAsync();
                }

                var compareExchangeValue =
                    await store.Operations.SendAsync(new GetCompareExchangeValueOperation<AtomicGuard>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/1")));
                compareExchangeValue.Metadata.Add(Constants.Documents.Metadata.Expires, DateTime.UtcNow.AddHours(1));
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<AtomicGuard>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/1"), compareExchangeValue.Value, compareExchangeValue.Index));

                using (var session = store.OpenAsyncSession(new SessionOptions() { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var r = await session.LoadAsync<TestObj>(id);
                    r.Prop = "Foo";
                    await session.SaveChangesAsync();
                }
            }
        }

        private class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        [Fact]
        public async Task CompareExchangeNestedMetadata_Create_WithoutPropChange()
        {
            using var store = GetDocumentStore();
            const string id = "testObjs/0";
            Dictionary<string, long> dictionary = new Dictionary<string, long> { { "bbbb", 2 } };

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata["Custom-Metadata"] = dictionary;
                await session.SaveChangesAsync();
            }

            //The session doesn't set the metadata but it can be seen in the studio. 
            WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                var metadata = entity.Metadata;
                var dictionary1 = metadata.GetObject("Custom-Metadata");
                Assert.Equal(2, dictionary1.GetLong("bbbb"));
            }
        }

        [Fact]
        public async Task CompareExchangeDoubleNestedMetadata_Create_WithoutPropChange()
        {
            using var store = GetDocumentStore();
            const string id = "testObjs/0";
            var dictionary = new Dictionary<string, Dictionary<string, int>>
            {
                {
                    "123", new Dictionary<string, int> { { "aaaa", 1 } }
                },
                {
                    "321", new Dictionary<string, int> { { "bbbb", 2 } }
                }
            };

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata["Custom-Metadata"] = dictionary;
                await session.SaveChangesAsync();
            }

            //The session doesn't set the metadata but it can be seen in the studio. 
            // WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                var metadata = entity.Metadata;
                var dictionaryLevel1 = metadata.GetObject("Custom-Metadata");

                var dictionaryLevel2 = dictionaryLevel1.GetObject("123");
                Assert.Equal(1, dictionaryLevel2.GetLong("aaaa"));

                dictionaryLevel2 = dictionaryLevel1.GetObject("321");
                Assert.Equal(2, dictionaryLevel2.GetLong("bbbb"));
            }
        }

    }
}
