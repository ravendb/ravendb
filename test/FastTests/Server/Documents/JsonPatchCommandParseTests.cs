using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents;

public class JsonPatchCommandParseTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Patching)]
    [InlineData("move", "from")]
    [InlineData("Move", "from")]
    [InlineData("COPY", "from")]
    [InlineData("Add", "value")]
    [InlineData("Replace", "value")]
    [InlineData("TEST", "value")]
    public void OperationsMissingRequiredPropertiesAreRejectedRegardlessOfCase(string op, string missingProperty)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject patch = context.Sync.ReadForMemory($"{{\"Operations\":[{{\"op\":\"{op}\",\"path\":\"/a\"}}]}}", "patch"))
        {
            var exception = Assert.Throws<ArgumentException>(() => JsonPatchCommand.Parse(patch));
            Assert.Contains($"'{missingProperty}'", exception.Message);
        }
    }
}
