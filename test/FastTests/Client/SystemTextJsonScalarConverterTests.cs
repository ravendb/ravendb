using System;
using System.Collections.Generic;
using System.Numerics;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class SystemTextJsonScalarConverterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private class HoldsBigInteger
    {
        public BigInteger Value { get; set; }
    }

    private class HoldsChar
    {
        public char Value { get; set; }

        public List<char> Many { get; set; }
    }

    private enum ByteEnum : byte
    {
        One = 1,
        Max = byte.MaxValue
    }

    private enum SByteEnum : sbyte
    {
        Negative = -5
    }

    private enum ShortEnum : short
    {
        Negative = short.MinValue
    }

    [Flags]
    private enum LongFlags : long
    {
        High = 1L << 40,
        Higher = 1L << 41
    }

    private enum ULongEnum : ulong
    {
        Max = ulong.MaxValue
    }

    private class HoldsEnums
    {
        public ByteEnum Byte { get; set; }

        public SByteEnum SByte { get; set; }

        public ShortEnum Short { get; set; }

        public LongFlags Flags { get; set; }

        public ULongEnum ULong { get; set; }

        public Dictionary<ByteEnum, int> Keyed { get; set; }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("42")]
    [InlineData("-42")]
    [InlineData("18446744073709551615")]
    [InlineData("12345678901234567890123")]
    [InlineData("-12345678901234567890123")]
    [InlineData("\"12345678901234567890123\"")]
    public void BigIntegersAreReadLikeNewtonsoft(string json)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":" + json + "}", "big-integer"))
        {
            var expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable);
            var actual = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable);

            Assert.Equal(BigInteger.Parse(json.Trim('"')), expected.Value);
            Assert.Equal(expected.Value, actual.Value);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void BigIntegersAreReadFromFloatingPointNumbers()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":1.5e3}", "big-integer"))
        {
            Assert.Equal(new BigInteger(1500), SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsBigInteger>(blittable).Value);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("\"a\"", 'a')]
    [InlineData("\"\\u00e9\"", '\u00e9')]
    [InlineData("\"\u017c\"", '\u017c')]
    [InlineData("\"\\\"\"", '"')]
    [InlineData("97", 'a')]
    public void CharsAreReadLikeNewtonsoft(string json, char expected)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":" + json + ",\"Many\":[" + json + "," + json + "]}", "char"))
        {
            var newtonsoft = Newtonsoft.Serialization.DefaultConverter.FromBlittable<HoldsChar>(blittable);
            var systemTextJson = SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsChar>(blittable);

            Assert.Equal(expected, newtonsoft.Value);
            Assert.Equal(expected, systemTextJson.Value);
            Assert.Equal(new[] { expected, expected }, systemTextJson.Many);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("\"ab\"")]
    [InlineData("\"\"")]
    [InlineData("\"\\uD83D\\uDE00\"")]
    [InlineData("\"this is far too long for one char\"")]
    public void CharsRejectStringsOfOtherLengths(string json)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Value\":" + json + "}", "char"))
        {
            Assert.ThrowsAny<Exception>(() => SystemTextJson.Serialization.DefaultConverter.FromBlittable<HoldsChar>(blittable));
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void CharsAreWrittenLikeNewtonsoft()
    {
        var value = new HoldsChar { Value = '\u017c', Many = ['a', '"', '\\', '\u00e9', '\0'] };

        var read = AssertWritesLikeNewtonsoft(value);
        Assert.Equal(value.Value, read.Value);
        Assert.Equal(value.Many, read.Many);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void CharMetadataValuesAreWrittenLikeNewtonsoft()
    {
        var metadata = new MetadataAsDictionary
        {
            ["Ascii"] = 'a',
            ["Wide"] = '\u017c'
        };

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = Newtonsoft.Serialization.DefaultConverter.ToBlittable(new HoldsChar(), metadata, context))
        using (BlittableJsonReaderObject actual = SystemTextJson.Serialization.DefaultConverter.ToBlittable(new HoldsChar(), metadata, context))
        {
            var expectedMetadata = (BlittableJsonReaderObject)expected[Raven.Client.Constants.Documents.Metadata.Key];
            var actualMetadata = (BlittableJsonReaderObject)actual[Raven.Client.Constants.Documents.Metadata.Key];

            Assert.Equal("a", actualMetadata["Ascii"].ToString());
            Assert.Equal("\u017c", actualMetadata["Wide"].ToString());
            Assert.Equal(expectedMetadata["Ascii"].ToString(), actualMetadata["Ascii"].ToString());
            Assert.Equal(expectedMetadata["Wide"].ToString(), actualMetadata["Wide"].ToString());
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public void EnumsOfEveryUnderlyingTypeAreWrittenAndReadLikeNewtonsoft(bool saveEnumsAsIntegers)
    {
        var value = new HoldsEnums
        {
            Byte = ByteEnum.Max,
            SByte = SByteEnum.Negative,
            Short = ShortEnum.Negative,
            Flags = LongFlags.High | LongFlags.Higher,
            ULong = ULongEnum.Max,
            Keyed = new Dictionary<ByteEnum, int> { [ByteEnum.One] = 1, [ByteEnum.Max] = 2 }
        };

        var read = saveEnumsAsIntegers ? AssertWritesLikeNewtonsoft(value, NewtonsoftIntegers, SystemTextJsonIntegers) : AssertWritesLikeNewtonsoft(value);
        Assert.Equal(value.Byte, read.Byte);
        Assert.Equal(value.SByte, read.SByte);
        Assert.Equal(value.Short, read.Short);
        Assert.Equal(value.Flags, read.Flags);
        Assert.Equal(value.ULong, read.ULong);
        Assert.Equal(value.Keyed, read.Keyed);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void EnumsReadOutOfRangeNumbersLikeEnumToObject()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory("{\"Byte\":300,\"SByte\":251,\"Short\":-32769,\"Flags\":-1}", "enums"))
        {
            var read = SystemTextJsonIntegers.Serialization.DefaultConverter.FromBlittable<HoldsEnums>(blittable);

            Assert.Equal((ByteEnum)Enum.ToObject(typeof(ByteEnum), 300L), read.Byte);
            Assert.Equal((SByteEnum)Enum.ToObject(typeof(SByteEnum), 251L), read.SByte);
            Assert.Equal((ShortEnum)Enum.ToObject(typeof(ShortEnum), -32769L), read.Short);
            Assert.Equal((LongFlags)Enum.ToObject(typeof(LongFlags), -1L), read.Flags);
        }
    }

    private static T AssertWritesLikeNewtonsoft<T>(T value)
    {
        return AssertWritesLikeNewtonsoft(value, Newtonsoft, SystemTextJson);
    }

    private static T AssertWritesLikeNewtonsoft<T>(T value, DocumentConventions newtonsoft, DocumentConventions systemTextJson)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject expected = newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        using (BlittableJsonReaderObject actual = systemTextJson.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            JObject expectedJson = JObject.Parse(expected.ToString());
            JObject actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected {expectedJson} but got {actualJson}");

            return systemTextJson.Serialization.DefaultConverter.FromBlittable<T>(actual);
        }
    }

    private static DocumentConventions Newtonsoft { get; } = new() { Serialization = new NewtonsoftJsonSerializationConventions() };

    private static DocumentConventions SystemTextJson { get; } = new() { Serialization = new SystemTextJsonSerializationConventions() };

    private static DocumentConventions NewtonsoftIntegers { get; } = new() { SaveEnumsAsIntegers = true, Serialization = new NewtonsoftJsonSerializationConventions() };

    private static DocumentConventions SystemTextJsonIntegers { get; } = new() { SaveEnumsAsIntegers = true, Serialization = new SystemTextJsonSerializationConventions() };
}
