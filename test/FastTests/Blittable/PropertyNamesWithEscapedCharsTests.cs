using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable;

public class PropertyNamesWithEscapedCharsTests : RavenTestBase
{
    public PropertyNamesWithEscapedCharsTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public async Task PropertyNameWithControlCharacter_WhenTryToAddUserLevelEscapedVersion_ShouldTreatThemAsDifferentValues()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        
        using var memoryStream = new MemoryStream();

        using (var withControlCharacter = context.GetLazyString("a\0a"))
        using (var withControlCharacterButEscapedInUserLevel = context.GetLazyString("a\\u0000a"))
        using (var withNewLineAndBack = context.GetLazyString("a\n\bb"))
        using (var withNewLineAndBackEscaped = context.GetLazyString("a\\n\\bb"))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, memoryStream))
        {
            writer.WriteStartObject();
            {
                writer.WritePropertyName(withControlCharacter);
                writer.WriteString("someValue1");
                
                writer.WritePropertyName(withControlCharacterButEscapedInUserLevel);
                writer.WriteString("someValue2");
                
                writer.WritePropertyName(withNewLineAndBack);
                writer.WriteString("someValue3");
                
                writer.WritePropertyName(withNewLineAndBackEscaped);
                writer.WriteString("someValue4");
            }
            writer.WriteEndObject();
        }

        memoryStream.Seek(0, SeekOrigin.Begin);
        using (var json = await context.ReadForMemoryAsync(memoryStream, "result"))
        {
            Assert.Equal(4, json.Count);
            Assert.Equal("{\"a\\u0000a\":\"someValue1\",\"a\\\\u0000a\":\"someValue2\",\"a\\n\\bb\":\"someValue3\",\"a\\\\n\\\\bb\":\"someValue4\"}", json.ToString());
        }
    }

    [RavenFact(RavenTestCategory.Core)]
    public async Task PropertyNameWithControlCharacter_WhenCache_ShouldNotCacheTwice()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();
        
        using var memoryStream = new MemoryStream();

        using (var propNameWithNull = context.GetLazyString("\na\0"))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, memoryStream))
        {
            writer.WriteStartObject();
            {
                writer.WritePropertyName(propNameWithNull);
                writer.WriteString("a\nc");
            }
            writer.WriteEndObject();
        }

        memoryStream.Seek(0, SeekOrigin.Begin);
        using (var _ = await context.ReadForMemoryAsync(memoryStream, "result"))
        {
        }
    
        memoryStream.Seek(0, SeekOrigin.Begin);
        //Fails here while reading.
        using (var _ = await context.ReadForMemoryAsync(memoryStream, "result"))
        {
        }
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public void PropertyNameWithControlCharacter_LoadingDictionaryKeyWithNullChar_ShouldNotThrowException()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1", StrDict = new Dictionary<string, string> { { "nullChar\0", "value" } } });
                session.SaveChanges();
            }
            using (var session = store.OpenSession())
            {
                var doc = session.Load<Doc>("doc-1");
                Assert.Equal("value", doc.StrDict["nullChar\0"]);
            }
        }
    }

    public class Doc
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
        public Dictionary<string, string> StrDict { get; set; }
    }
}
