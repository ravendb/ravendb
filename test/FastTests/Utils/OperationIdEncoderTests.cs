using FastTests.Voron.FixedSize;
using Raven.Server.Utils;
using Xunit;

namespace FastTests.Utils;

public class OperationIdEncoderTests
{
    [Theory]
    [InlineData("?", 1)]
    [InlineData("?", 8_589_934_591)]
    [InlineDataWithRandomSeed("?")]
    [InlineDataWithRandomSeed("D")]
    [InlineData("ZZZZ", 1)]
    [InlineData("ZZZZ", 8_589_934_591)]
    [InlineDataWithRandomSeed("ZZZZ")]
    [InlineData("ZZZZ", 8_589_934_592)]
    [InlineData("ZZZZ", 8_589_934_591_999)]
    [InlineData("ZZZZ", long.MaxValue)]
    public void OperationIdEncoder_Should_Work(string nodeTag, long operationId)
    {
        long expectedOperationId = operationId;
        if (operationId > OperationIdEncoder.MaxOperationId)
            expectedOperationId = operationId % OperationIdEncoder.MaxOperationId;

        var encodedOperationId = OperationIdEncoder.EncodeOperationId(operationId, nodeTag);

        Assert.Equal(expectedOperationId, OperationIdEncoder.DecodeOperationId(encodedOperationId, out var decodedNodeTag));
        Assert.Equal(nodeTag, decodedNodeTag);
    }
}
