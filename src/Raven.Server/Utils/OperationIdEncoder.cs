using System.Diagnostics;
using System.Text;

namespace Raven.Server.Utils;

public static class OperationIdEncoder
{
    private const int BaseChar = '?' - 1;

    private const int NumberOfBitsNeededToEncodeSingleNodeTagChar = 5;

    private const int MaxNumberOfBitsUsedForOperationId = 33;

    private const int MaxNodeTagLength = 4;

    private const long MaxEncodedId = 9_007_199_254_740_991;

    public const long MaxOperationId = long.MaxValue & 0b000111111111111111111111111111111111;

    /// <summary>
    /// JavaScript Number.MAX_SAFE_INTEGER is 9007199254740991 which is less than 8 bytes that C# is using
    /// NodeTag can be ? or A-Z which means if we will use '?' as base we have numbers between 0 and 27 they can be encoded in 5 bits
    /// Max NodeTag length is 4 so we need to use 20 bits to encode this
    /// If we will use NodeTag 'ZZZZ' and 8_589_934_591 as operationId we will end up with 8135535642017791 which is slightly less than Number.MAX_SAFE_INTEGER
    /// 7844981104443391 is binary is:
    /// 0000 0000 | 0001 1011 | 1101 1110 | 1111 0111 | 1111 1111 | 1111 1111 | 1111 1111 | 1111 1111
    ///
    /// Which results in:
    /// - 33 bits we can use for operation ID
    /// - 20 bits we need to encode nodeTag
    /// - 11 bits that are wasted due to Number.MAX_SAFE_INTEGER
    /// </summary>
    public static long EncodeOperationId(long operationId, string nodeTag)
    {
        if (operationId > MaxOperationId)
            operationId %= MaxOperationId;

        var nodeTagLongs = ConvertToLongs(nodeTag);

        for (var i = MaxNodeTagLength - 1; i >= 0; --i)
            operationId |= nodeTagLongs[i] << (MaxNumberOfBitsUsedForOperationId + (NumberOfBitsNeededToEncodeSingleNodeTagChar * i));

        Debug.Assert(operationId <= MaxEncodedId, $"{operationId} > {MaxEncodedId}");

        return operationId;

        static long[] ConvertToLongs(string nodeTag)
        {
            byte[] nodeTagBytes = Encoding.ASCII.GetBytes(nodeTag);
            long[] nodeTagLongs = new long[MaxNodeTagLength];
            for (long i = 0; i < nodeTagBytes.Length; i++)
                nodeTagLongs[i] = nodeTagBytes[i] - BaseChar;

            return nodeTagLongs;
        }
    }

    public static long DecodeOperationId(long input, out string nodeTag)
    {
        nodeTag = string.Empty;

        var operationId = input & 0b000111111111111111111111111111111111;

        input = ((input << 8) >> 8) >> MaxNumberOfBitsUsedForOperationId;

        static char ExtractChar(ref long input)
        {
            var ch = (char)((input & 0b00011111) + BaseChar);
            input >>= NumberOfBitsNeededToEncodeSingleNodeTagChar;

            return ch;
        }

        for (var i = 0; i < MaxNodeTagLength; i++)
        {
            var ch = ExtractChar(ref input);
            if (ch > BaseChar)
                nodeTag += ch;
        }

        return operationId;
    }
}

