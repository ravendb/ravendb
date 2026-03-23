using System.Linq;
using System.Text;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow;

public unsafe  class LazyStringValueTests : NoDisposalNeeded
{
    public LazyStringValueTests(ITestOutputHelper output) : base(output)
    {
    }

    public static TheoryData<string> LazyStringTestCases =>
    [
        new string('\u0001', 1),
        new string('\u0001', 10),
        string.Concat(Enumerable.Repeat("\u0001a", 1)),
        string.Concat(Enumerable.Repeat("\u0001a", 10)),
        '\n' + string.Concat(Enumerable.Repeat("\u0001a", 1)),
        '\n' + string.Concat(Enumerable.Repeat("\u0001a", 10)),
        string.Concat(Enumerable.Repeat("\u0001a", 1)) + '\n',
        string.Concat(Enumerable.Repeat("\u0001a", 10)) + '\n',
        string.Concat(Enumerable.Repeat("\u0001\n", 10))
    ];
    
    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(LazyStringTestCases))]
    public void LazyStringValue_WhenToStringForJsonType_ShouldResultWithUnescapedValue(string expected)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        
        var lazyString = context.GetLazyString(expected);
        lazyString = context.AllocateStringValue(null, lazyString.Buffer, lazyString.Size, LazyStringType.JsonString);
        
        var actual = lazyString.ToString();
        Assert.Equal(expected, actual);
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(LazyStringTestCases))]
    public void LazyStringValue_WhenToStringForSimpleType_ShouldResultWithUnescapedValue(string expected)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var lazyString = context.GetLazyString(expected);
        lazyString = context.AllocateStringValue(null, lazyString.Buffer, lazyString.Size, LazyStringType.SimpleString);
        
        var actual = lazyString.ToString();
        Assert.Equal(expected, actual);
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(LazyStringTestCases))]
    public void LazyStringValue_WhenEqualsSimpleTypeWithJsonType_ShouldReturnTrue(string value)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        
        var expected = context.GetLazyString(value);

        using var blittable = context.ReadObject(new DynamicJsonValue { ["Prop"] = value }, "doc");
        var actual = (LazyStringValue)blittable["Prop"];
        // actual.GetHashCode();

        Assert.Equal(expected, actual);
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(LazyStringTestCases))]
    public void LazyStringValue_WhenCompareJsonTypes_ShouldReturnTrue(string value)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        
        var jsonValue = context.GetLazyString(value);
        jsonValue = context.AllocateStringValue(null, jsonValue.Buffer, jsonValue.Size, LazyStringType.JsonString);
        
        using var blittable = context.ReadObject(new DynamicJsonValue { ["Prop"] = value }, "doc");
        var simpleValue = (LazyStringValue)blittable["Prop"];
        
        Assert.Equal(0, jsonValue.CompareTo(simpleValue));
    }
    
    
    public static TheoryData<string, string, int[]> LazyStringPositionsTestData =>
        new TheoryData<string, string, int[]>
        {
            { null, @"C:\work\raven", [2, 4] },
            { null, @"C:\work\raven\RavenDB-24955\sln\test\FastTests\bin\Debug\net8.0\Databases\CRUD_Operations_1.0-3", [2, 4, 5, 13, 3, 4, 9, 3, 5, 6, 9] },
            { null, @"01\123\u0001", [2, 4] },
            { "01\\123\u0001", @"01\123\u0001", [2] }
        };

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(LazyStringPositionsTestData))]
    public void LazyStringValue_ShouldCorrectlyHandleEscapePositions(string expected, string value, int[] escapePositions)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        
        var lazyString = GetLazyString(context, Encoding.UTF8.GetBytes(value), LazyStringType.SimpleString);
        lazyString.EscapePositions = escapePositions;
            
        Assert.Equal(expected ?? value, lazyString);
    }
    
    public static TheoryData<int, string, int[]> EscapedUnicodeControlCharactersTestData =>
        new TheoryData<int, string, int[]>
        {
            { 0, "1\t\\1\\\\2\\", [1, 0, 1, 0, 1] },
            { 1, "1\t\\1\\u0001\\\\2\\", [1, 0, 7, 0, 1] },
            { 0, "1\t\\1\\u0001\\\\2\\", [1, 0, 1, 5, 0, 1] },
        };

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(EscapedUnicodeControlCharactersTestData))]
    public void CountEscapedUnicodeControlCharacters_ShouldReturnCorrectCount(int expected, string value, int[] escapePositions)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        var lazyString = GetLazyString(context, Encoding.UTF8.GetBytes(value), LazyStringType.JsonString);

        var actual = StringUtils.CountEscapedControlCharacters(lazyString.AsSpan(), escapePositions);
            
        Assert.Equal(expected , actual);
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public void BlittableJsonReaderObject_TryGet_ShouldCorrectlyRetrieveLazyStringValue_WithUnicodeCharacters()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        const string prop = "b\u0001a1";
        const string value = "b\u0002a2";
        var obj = new DynamicJsonValue
        {
            [prop] = value
        };
        using (var json = context.ReadObject(obj, "obj", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
        {
            Assert.True(json.TryGet(prop, out LazyStringValue lazyStringValue), "Property not found");
            Assert.True(lazyStringValue.Equals(value));
            Assert.Equal(value, lazyStringValue);
        }
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public void BlittableJsonReaderObject_WithModifications_ShouldCorrectlyRetrieveLazyStringValue_WithUnicodeCharacters()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        const string collection = "b\u0001a1";
        const string toRemove = "someprop";
        var obj = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = collection,
                [toRemove] = "somevalue",
            }
        };
        using var json1 = context.ReadObject(obj, "obj", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        Assert.True(json1.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata), "Property not found");
        metadata.Modifications = new DynamicJsonValue(metadata);
        metadata.Modifications.Remove(toRemove);
        
        using var modifiedMetadata = context.ReadObject(metadata, "metadata", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        
        Assert.True(modifiedMetadata.TryGet(Constants.Documents.Metadata.Collection, out LazyStringValue lazyStringValue), "Property not found");
        Assert.True(lazyStringValue.Equals(collection));
        Assert.Equal(collection, lazyStringValue);
    }

    private static LazyStringValue GetLazyString(JsonOperationContext context, byte[] buffer, LazyStringType type)
    {
        fixed (byte* pBytes = buffer)
        {
            return context.AllocateStringValue(null, pBytes, buffer.Length, type);
        }
    }
}
