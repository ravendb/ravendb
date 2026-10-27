using Newtonsoft.Json;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class JavascriptStringLiteralTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Patching)]
    public void EveryCharacterIsEscapedLikeJsonConvert()
    {
        for (int i = char.MinValue; i <= char.MaxValue; i++)
        {
            string value = "a" + (char)i + "b";
            Assert.Equal(JsonConvert.ToString(value), JavascriptConversionExtensions.ToJsStringLiteral(value));
        }
    }

    [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Patching)]
    [InlineData("")]
    [InlineData("plain")]
    [InlineData("key\"with\"quotes")]
    [InlineData("back\\slash and 'single' quotes")]
    [InlineData("lines\r\n\tand\u2028separators\u2029")]
    [InlineData("\u0000\u0001\u001f\u007f\u0085")]
    [InlineData("</script><!-- & -->")]
    [InlineData("\u017c\u00f3\u0142\u0107 \ud83d\ude00")]
    public void StringsAreEscapedLikeJsonConvert(string value)
    {
        Assert.Equal(JsonConvert.ToString(value), JavascriptConversionExtensions.ToJsStringLiteral(value));
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Patching)]
    public void NullIsWrittenAsNullLiteral()
    {
        Assert.Equal("null", JavascriptConversionExtensions.ToJsStringLiteral(null));
    }
}
