#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;
#pragma warning disable CS8603
public class DynamicFieldsIntegration : RavenTestBase
{
    private const int DataToIndex = 100;

    public DynamicFieldsIntegration(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleStringDynamicFieldsTest(Options options)
    {
        using var store = await GetStoreWithIndexAndData<IndexString, string>(options, i => $"Container no {i}");
        WaitForUserToContinueTheTest(store);
        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<string>, IndexString>().Where(i => i.Dynamic == $"Container no 50").SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleStringNullDynamicFieldsTest(Options options)
    {
        using var store = await GetStoreWithIndexAndData<IndexString, string>(options, i => i == 50 ? null : $"Container no {i}");
        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<string?>, IndexString>().Where(i => i.Dynamic == null).SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleStringListNullDynamicFieldsTest(Options options)
    {
        using var store =
            await GetStoreWithIndexAndData<IndexStringList, string[]>(options, i => i == 50 ? null : Enumerable.Range(0, 10).Select(p => $"Container no {p}").ToArray());
        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<string?>, IndexStringList>().Where(i => i.Dynamic == null).SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleFloatDynamicFieldsTest(Options options)
    {
        using var store = await GetStoreWithIndexAndData<IndexFloat, float?>(options, i => (float)i + float.Epsilon);

        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<float?>, IndexFloat>().Where(i => i.Dynamic!.Value == 50).SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleFloatWithNullDynamicFieldsTest(Options options)
    {
        using var store = await GetStoreWithIndexAndData<IndexFloat, float?>(options, i => i == 50 ? null : (float)i + float.Epsilon);

        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<float?>, IndexFloat>().Where(i => i.Dynamic == null).SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task ListFloatDynamicFieldsTest(Options options)
    {
        using var store = await GetStoreWithIndexAndData<IndexFloatList, float?[]>(options,
            i => i == 50 ? new float?[] {1.0f, 2.0f, 50.0f} : Enumerable.Range(0, 10).Select(p => (float?)p + float.Epsilon).ToArray());
        
        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<float?[]>, IndexFloatList>().Where(i => i.Dynamic.Contains(50)).SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task ListFloatWithNullDynamicFieldsTest(Options options)
    {
        using var store = await GetStoreWithIndexAndData<IndexFloatList, float?[]>(options,
            i => i == 50 ? null : Enumerable.Range(0, 10).Select(p => (float?)p + float.Epsilon).ToArray());

        {
            using var session = store.OpenAsyncSession();
            var result = await session.Query<TestClassResult<float?>, IndexFloatList>().Where(i => i.Dynamic == null).SingleOrDefaultAsync();
            AssertResult(result);
        }
    }

    private async Task<IDocumentStore> GetStoreWithIndexAndData<TIndex, T>(Options options, Func<int, T> creator)
        where TIndex : AbstractIndexCreationTask, new()
    {
        var index = new TIndex();
        var testData = new List<TestClass<T>>();
        for (int i = 0; i < DataToIndex; ++i)
        {
            testData.Add(new TestClass<T>() {Id = $"data/{i}", Value = creator(i)});
        }

        var store = GetDocumentStore(options);

        {
            await using var bulk = store.BulkInsert();
            foreach (var data in testData)
            {
                await bulk.StoreAsync(data);
            }
        }

        await index.ExecuteAsync(store);
        Indexes.WaitForIndexing(store);
        WaitForUserToContinueTheTest(store);
        return store;
    }

    private void AssertResult<T>(TestClassResult<T> result)
    {
        Assert.NotNull(result);
        Assert.Equal("data/50", result.Id);
    }

    private class TestClass<T>
    {
        public string? Id { get; set; }
        public T? Value { get; set; }
    }

    private class TestClassResult<T>
    {
        public string? Id { get; set; }
        public T? Value { get; set; }
        public T? Dynamic { get; set; }
    }


    private class IndexFloat : AbstractIndexCreationTask<TestClass<float?>, TestClassResult<float?>>
    {
        public IndexFloat()
        {
            Map = classes => classes.Select(i => new {Id = i.Id, _ = CreateField("Dynamic", i.Value)});
        }
    }

    private class IndexFloatList : AbstractIndexCreationTask<TestClass<float?[]>, TestClassResult<float?[]>>
    {
        public IndexFloatList()
        {
            Map = classes => classes.Select(i => new {Id = i.Id, _ = CreateField("Dynamic", i.Value)});
        }
    }

    private class IndexString : AbstractIndexCreationTask<TestClass<string>, TestClassResult<string>>
    {
        public IndexString()
        {
            Map = classes => classes.Select(i => new {Id = i.Id, _ = CreateField("Dynamic", i.Value)});
        }
    }

    private class IndexStringList : AbstractIndexCreationTask<TestClass<string[]>, TestClassResult<string[]>>
    {
        public IndexStringList()
        {
            Map = classes => classes.Select(i => new {Id = i.Id, _ = CreateField("Dynamic", i.Value)});
        }
    }
}
#pragma warning restore CS8603
