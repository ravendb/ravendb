using System;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using FastTests.Voron.FixedSize;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class BlittableVectorIntegrationTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesByte(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (byte)random.Next(byte.MinValue, byte.MaxValue + 1)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (byte)random.Next(byte.MinValue, byte.MaxValue + 1)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesUshort(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (ushort)random.Next(ushort.MinValue, ushort.MaxValue + 1)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (ushort)random.Next(ushort.MinValue, ushort.MaxValue + 1)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesUInt(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (uint)random.NextInt64(uint.MinValue, (long)uint.MaxValue + 1)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (uint)random.NextInt64(uint.MinValue, (long)uint.MaxValue + 1)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesULong(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (ulong)~random.NextInt64(long.MinValue, long.MaxValue)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (ulong)~random.NextInt64(long.MinValue, long.MaxValue)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesSByte(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesShort(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (short)random.Next(short.MinValue, short.MaxValue + 1)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (short)random.Next(short.MinValue, short.MaxValue + 1)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesInt(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (int)random.NextInt64(int.MinValue, (long)int.MaxValue + 1)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (int)random.NextInt64(int.MinValue, (long)int.MaxValue + 1)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesLong(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => random.NextInt64(long.MinValue, long.MaxValue)).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => random.NextInt64(long.MinValue, long.MaxValue)).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesHalf(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => (Half)random.NextSingle() * Half.MaxValue).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => (Half)random.NextSingle()).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesFloat(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => random.NextSingle()).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => random.NextSingle()).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(8)]
    [InlineDataWithRandomSeed(500)]
    [InlineDataWithRandomSeed(1024)]
    [InlineDataWithRandomSeed(2048)]
    public async Task CanReadGenericValuesDouble(int size, int seed)
    {
        var random = new Random(seed);
        var array1 = Enumerable.Range(0, size).Select(_ => random.NextSingle() * double.MaxValue).ToArray();
        var array2 = Enumerable.Range(0, size).Select(_ => random.NextSingle() * double.MaxValue).ToArray();
        await CanReadGenericValuesBase(array1, array2);
    }

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(1.0d, 8)]
    [InlineData(1.0d, 500)]
    [InlineData(1.0d, 1024)]
    [InlineData(1.0d, 2048)]
    [InlineData(1.1d, 8)]
    [InlineData(1.1d, 500)]
    [InlineData(1.1d, 1024)]
    [InlineData(1.1d, 2048)]
    public async Task CanReadFloatingValuesAsAllTypes(double repeatedValue, int size)
    {
        var array1 = Enumerable.Repeat(1d, size).ToArray();
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenAsyncSession(new SessionOptions(){NoCaching = true}))
        {
            var vectorDto = new GenericUser<double>(){Vector = array1};
            await session.StoreAsync(vectorDto);
            await session.SaveChangesAsync();
            id = vectorDto.Id;
        }
        
        //Double
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<double>>(id);
            Assert.Equal(array1.Select(x => (double)x), storedUser.Vector.Embedding);
        }
        
        //Float
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<float>>(id);
            Assert.Equal(array1.Select(x => (float)x), storedUser.Vector.Embedding);
        }
        
        //Half
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<Half>>(id);
            Assert.Equal(array1.Select(x => (Half)x), storedUser.Vector.Embedding);
        }
        
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(8)]
    [InlineData(500)]
    [InlineData(1024)]
    [InlineData(2048)]
    public async Task CanReadNumericValuesAsAllTypes(int size)
    {
        var array1 = Enumerable.Repeat(1, size).ToArray();
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenAsyncSession(new SessionOptions(){NoCaching = true}))
        {
            var vectorDto = new GenericUser<int>(){Vector = array1};
            await session.StoreAsync(vectorDto);
            await session.SaveChangesAsync();
            id = vectorDto.Id;
        }
        
        //SByte
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<sbyte>>(id);
            Assert.Equal(array1.Select(x => (sbyte)x), storedUser.Vector.Embedding);
        }
        
        //Short
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<short>>(id);
            Assert.Equal(array1.Select(x => (short)x), storedUser.Vector.Embedding);
        }
        
        //Int
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<int>>(id);
            Assert.Equal(array1.Select(x => (int)x), storedUser.Vector.Embedding);
        }
        
        //Long
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<long>>(id);
            Assert.Equal(array1.Select(x => (long)x), storedUser.Vector.Embedding);
        }
        
        //SByte
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<byte>>(id);
            Assert.Equal(array1.Select(x => (byte)x), storedUser.Vector.Embedding);
        }
        
        //UShort
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<ushort>>(id);
            Assert.Equal(array1.Select(x => (ushort)x), storedUser.Vector.Embedding);
        }
        
        //UInt
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<uint>>(id);
            Assert.Equal(array1.Select(x => (uint)x), storedUser.Vector.Embedding);
        }
        
        //ULong
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<ulong>>(id);
            Assert.Equal(array1.Select(x => (ulong)x), storedUser.Vector.Embedding);
        }
    }
    
    private async Task CanReadGenericValuesBase<T>(T[] array1, T[] array2) where T : unmanaged, INumber<T>
    {
        using var store = GetDocumentStore();
        string id;
        using (var session = store.OpenAsyncSession(new SessionOptions(){NoCaching = true}))
        {
            var vectorDto = new GenericUser<T>(){Vector = array1, Vector2 = array2};
            await session.StoreAsync(vectorDto);
            await session.SaveChangesAsync();
            id = vectorDto.Id;
        }
        
        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var storedUser = await session.LoadAsync<GenericUser<T>>(id);
            Assert.Equal(array1, storedUser.Vector.Embedding);
            Assert.Equal(array2, storedUser.Vector2.Embedding);
        }
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.JavaScript)]
    public async Task CanReadBlittableVector()
    {
        using var store = GetDocumentStore();
        string id;
        var array = new[] { 0.1f, 0.1f, 0.1f, 0.1f };
        var array2 = array.Select(x => x * 2f).ToArray();
        using (var session = store.OpenAsyncSession(new SessionOptions(){NoCaching = true}))
        {
            var vectorDto = new User(){Vector = array, CopiedVector = array2};
            await session.StoreAsync(vectorDto);
            await session.SaveChangesAsync();
            id = vectorDto.Id;
        }

        using (var session = store.OpenAsyncSession(new SessionOptions() { NoCaching = true }))
        {
            var valueFromServer = await session.LoadAsync<User>(id);
            Assert.Equal(array, valueFromServer.Vector.Embedding);
            Assert.Equal(array2, valueFromServer.CopiedVector.Embedding);
        }        
    }

    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.JavaScript | RavenTestCategory.Patching)]
    public async Task CanPatchWithBlittableVector()
    {
        using var store = GetDocumentStore();
        string id;
        var array = new[] { 0.1f, 0.1f, 0.1f, 0.1f };
        using (var session = store.OpenAsyncSession(new SessionOptions(){NoCaching = true}))
        {
            var vectorDto = new User(){Vector = array};
            await session.StoreAsync(vectorDto);
            await session.SaveChangesAsync();
            id = vectorDto.Id;
        }
        
        var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Users update { this.CopiedVector = this.Vector; }"));
        await operation.WaitForCompletionAsync();

        using (var session = store.OpenAsyncSession(new SessionOptions(){NoCaching = true, NoTracking = true}))
        {
            await Task.Delay(TimeSpan.FromSeconds(0.5));
            var valueFromServer = await session.LoadAsync<User>(id);
            Assert.Equal(array, valueFromServer.CopiedVector.Embedding);
        }
    }

    private class User
    {
        public RavenVector<float> Vector { get; set; }
        public RavenVector<float> CopiedVector { get; set; }
        public string Id { get; set; }
    }
    
    private class GenericUser<T> where T : unmanaged, INumber<T>
    {
        public RavenVector<T> Vector { get; set; }
        public RavenVector<T> Vector2 { get; set; }
        public string Id { get; set; }
    }
}
