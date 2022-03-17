using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_18334 : NoDisposalNeeded
{
    public RavenDB_18334(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Can_Build_Serializator_For_SmugglerResult()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            var result = new SmugglerResult();
            result.AddError("MessageA");
            result.AddInfo("MessageB");
            result.AddWarning("MessageC");
            result.AddMessage("MessageD");

            var djv = result.ToJson();

            var json = context.ReadObject(djv, "smuggler/result");

            var result2 = JsonDeserializationClient.SmugglerResult(json);

            Assert.Equal(result.Messages, result2.Messages);

            var result3 = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<SmugglerResult>(json);

            Assert.Equal(result.Messages, result3.Messages);
        }
        
    }
}
