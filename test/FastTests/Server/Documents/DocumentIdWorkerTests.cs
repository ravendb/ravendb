using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentIdWorkerTests : RavenTestBase
    {
        public DocumentIdWorkerTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Memory)]
        public async Task GetLower_WhenStringAscii_ShouldNotModifyTheValueAcceptToLower()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    for (var i = 32; i <= 127; i++)
                    {
                        using (var str = ctx.GetLazyString("Person@1" + (char)i))
                        {
                            using (DocumentIdWorker.GetLower(ctx.Allocator, str, out var lowerId))
                            {
                                Assert.Equal(str.ToLower(), lowerId.ToString());
                            }
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public async Task GetSliceFromId_WhenStringAscii_ShouldNotModifyTheValueAcceptToLower()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    const string str = "Person@1";

                    using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, str, out var lowerId))
                    {
                        Assert.Equal(str.ToLower(), lowerId.ToString());
                    }
                }
            }
        }
        
        [RavenFact(RavenTestCategory.Memory)]
        public void GetSliceFromId_WhenEmptyLazyString_ShouldNotThrow()
        {
            using var ctx = DocumentsOperationContext.ShortTermSingleUse(null);
            const string str = "";
            var lazyString = ctx.GetLazyString(str);
            using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, lazyString, out var lowerId))
            {
                Assert.Equal(str.ToLower(), lowerId.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public async Task GetSliceFromId_WhenStringIsUnicode_ShouldNotModifyTheValueAcceptToLower()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    const string str = "Person@יפתח";

                    using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, str, out var lowerId))
                    {
                        Assert.Equal(str.ToLower(), lowerId.ToString());
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public async Task GetSliceFromId_WhenDisposing_ShouldFreeMemory()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    var before = ctx.AllocatedMemory;
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, "Person@יפתח", out var lowerId))
                    {
                    }
                    var after = ctx.AllocatedMemory;

                    Assert.Equal(before, after);
                }
            }
        }

        private class TestObj
        {
            public string Id { get; set; }
        }

        public static object[][] Ids =>
            new object[][]
            {
                ["\0{\r\n>"], 
                [new string('\0', AbstractPager.MaxKeySize / (JsonParserState.ControlCharacterItemSize + 1) - 2) + '\n'], 
                ['a' + new string('\r', AbstractPager.MaxKeySize / (JsonParserState.EscapePositionItemSize + 1) - 4) + '\n']
            };

        [RavenTheory(RavenTestCategory.Memory)]
        [MemberData(nameof(Ids))]
        public async Task DocumentId_WhenWrite_ShouldBeAbleToRead(string id)
        {
            const char nonAscii = 'Ć';

            var idWithNonAscii = id + nonAscii;
            
            using var context = JsonOperationContext.ShortTermSingleUse();
            using var memoryStream = new MemoryStream();

            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(allocator, id, out var withoutAsciiSliceLower, out var withoutAsciiSlice))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(allocator, idWithNonAscii, out var withAsciiSliceLower, out var withAsciiSlice))
            {
                var withoutAsciiLazyString = GetLazyStringValue(context, withoutAsciiSlice);
                var withAsciiLazyString = GetLazyStringValue(context, withAsciiSlice);
            
            
                await using (var writer = new AsyncBlittableJsonTextWriter(context, memoryStream))
                {
                    Assert.True(withAsciiLazyString.StartsWith(withoutAsciiLazyString));

                    writer.WriteStartObject();
                    writer.WritePropertyName("withoutAsciiSlice");
                    writer.WriteString(withoutAsciiLazyString);
                    writer.WriteComma();
                    writer.WritePropertyName("withAsciiSlice");
                    writer.WriteString(withAsciiLazyString);
                    writer.WriteEndObject();
                }

                memoryStream.Seek(0, SeekOrigin.Begin);
                using (var reader = await context.ReadForMemoryAsync(memoryStream, "result"))
                {
                    Assert.True(reader["withoutAsciiSlice"].Equals(id));
                    Assert.True(reader["withAsciiSlice"].Equals(idWithNonAscii));
                }

                using (DocumentIdWorker.GetLoweredIdSliceFromId(allocator, id, out Slice withoutAsciiSlice2))
                using (DocumentIdWorker.GetLoweredIdSliceFromId(allocator, idWithNonAscii, out Slice withAsciiSlice2))
                {
                    Assert.Equal(withoutAsciiSliceLower, withoutAsciiSlice2, new SliceComparer());
                    Assert.Equal(withAsciiSliceLower, withAsciiSlice2, new SliceComparer());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Memory)]
        [MemberData(nameof(Ids))]
        public async Task DocumentId_WhenStore_ShouldBeAbleToLoadAndDelete(string id)
        {
            var idWithNonAscii = (char)(DocumentIdWorker.MaxAsciiCodePoint + 1) + id;
            
            using var store = GetDocumentStore();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), id);
                await session.StoreAsync(new TestObj(), idWithNonAscii);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                Assert.NotNull(await session.LoadAsync<TestObj>(id));
                Assert.NotNull(await session.LoadAsync<TestObj>(idWithNonAscii));
            }
            
            using (var session = store.OpenAsyncSession())
            {
                session.Delete(id);
                session.Delete(idWithNonAscii);
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession())
            {
                Assert.Null(await session.LoadAsync<TestObj>(id));
                Assert.Null(await session.LoadAsync<TestObj>(idWithNonAscii));
            }
        }

        private static unsafe LazyStringValue GetLazyStringValue(JsonOperationContext context, Slice idSlice)
        {
            var ret = context.GetLazyStringValue(idSlice.Content.Ptr, out var success);
            Assert.True(success);
            return ret;
        }
    }
}
