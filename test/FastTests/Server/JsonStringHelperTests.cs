using System.IO;
using Newtonsoft.Json;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server;

public class JsonStringHelperTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Configuration)]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("{\"ServerUrl\":\"https://a.example.com:443\",\"Setup.Mode\":\"LetsEncrypt\",\"DataDir\":\"C:\\\\Raven\\\\Data\"}")]
    [InlineData("{\"Security\":{\"Certificate\":{\"Path\":\"cluster.server.certificate.pfx\",\"Password\":null}},\"Ports\":[443,38888],\"Enabled\":true,\"Disabled\":false}")]
    [InlineData("{\"Name\":\"Zażółć gęślą jaźń <b>&'\\\"+\",\"Empty\":{},\"None\":[],\"Nested\":[{\"A\":1},{\"B\":[1,2,{\"C\":-3}]}]}")]
    [InlineData("{\"Id\":\"a5a5a5a5-0000-0000-0000-000000000000\",\"Keys\":[\"abc+/==\"],\"MaxClusterSize\":3,\"Ratio\":1.5}")]
    public void IndentsLikeNewtonsoft(string json)
    {
        Assert.Equal(IndentWithNewtonsoft(json), JsonStringHelper.Indent(json));
    }

    private static string IndentWithNewtonsoft(string json)
    {
        using (var stringReader = new StringReader(json))
        using (var stringWriter = new StringWriter())
        {
            var jsonReader = new JsonTextReader(stringReader) { DateParseHandling = DateParseHandling.None };
            var jsonWriter = new JsonTextWriter(stringWriter) { Formatting = Formatting.Indented };
            jsonWriter.WriteToken(jsonReader);
            return stringWriter.ToString();
        }
    }
}
