using System;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class JsonDeserializationTest : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, SerializationDeserializationValidation.Values> SerializationDeserializationValidation = GenerateJsonDeserializationRoutine<SerializationDeserializationValidation.Values>();
    }

    public class SerializationDeserializationValidation : NoDisposalNeeded
    {
        public SerializationDeserializationValidation(ITestOutputHelper output) : base(output)
        {
        }

        public class Values
        {
            public int intMinVal;
            public int intMaxVal;
            public long longMinVal;
            public long longMaxVal;
            public double doubleMinVal;
            public double doubleMaxVal;
            public double doubleNegativeInfinity;
            public double doublePositiveInfinity;
            public double doubleNan;
            public double doubleEpsilon;
            public float floatMinVal;
            public float floatMaxVal;
            public float floatMaxPercision;
            public float floatPositiveInfinity;
            public float floatNegativeInfinity;
            public float floatNan;
            public uint uintMaxVal;
            public ulong ulongMaxVal;
            public string stringMaxLength;
            public DateTime dateMaxPercision;
            public DateTimeOffset dateTimeOffsetMinVal;
            public DateTimeOffset dateTimeOffsetMaxVal;
            public TimeSpan timeSpanMinVal;
            public TimeSpan timeSpanMaxVal;
            public TimeSpan timeSpanDays;
            public TimeSpan timeSpanHours;
            public TimeSpan timeSpanMinutes;
            public TimeSpan timeSpanSeconds;
            public TimeSpan timeSpanMiliseconds;
            public TimeSpan timeSpanNanoseconds;
            public string[] stringArray;
        }

        [Fact]
        public void ValidateRanges()
        {
            var values = new Values
            {
                intMinVal = Int32.MinValue,
                intMaxVal = Int32.MaxValue,
                longMinVal = long.MinValue,
                longMaxVal = long.MaxValue,
                doubleMinVal = double.MinValue,
                doubleMaxVal = double.MaxValue,
                doubleNegativeInfinity = double.NegativeInfinity,
                doublePositiveInfinity = double.PositiveInfinity,
                doubleNan = double.NaN,
                doubleEpsilon = double.Epsilon,
                floatMinVal = float.MinValue,
                floatMaxVal = float.MaxValue,
                floatMaxPercision = float.Epsilon,
                floatNegativeInfinity = float.NegativeInfinity,
                floatPositiveInfinity = float.PositiveInfinity,
                floatNan = float.NaN,
                uintMaxVal = uint.MaxValue,
                ulongMaxVal = ulong.MaxValue,
                stringMaxLength = string.Join("", Enumerable.Repeat(1, short.MaxValue)),
                dateMaxPercision = DateTime.Now,
                dateTimeOffsetMinVal = DateTimeOffset.MinValue,
                dateTimeOffsetMaxVal = DateTimeOffset.MaxValue,
                timeSpanMinVal = TimeSpan.MinValue,
                timeSpanMaxVal = TimeSpan.MaxValue,
                timeSpanDays = TimeSpan.FromDays(1),
                timeSpanHours = TimeSpan.FromHours(1),
                timeSpanMinutes = TimeSpan.FromMinutes(1),
                timeSpanSeconds = TimeSpan.FromSeconds(1),
                timeSpanMiliseconds = TimeSpan.FromMilliseconds(1),
                timeSpanNanoseconds = TimeSpan.FromTicks(1),
                stringArray = new []{ "test", null, string.Empty }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var blittableValues = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(values, context);

                var valuesDeserialized = JsonDeserializationTest.SerializationDeserializationValidation(blittableValues);

                Assert.Equal(values.intMinVal, valuesDeserialized.intMinVal);
                Assert.Equal(values.intMaxVal, valuesDeserialized.intMaxVal);
                Assert.Equal(values.longMinVal, valuesDeserialized.longMinVal);
                Assert.Equal(values.longMaxVal, valuesDeserialized.longMaxVal);
                Assert.Equal(values.doubleMinVal, valuesDeserialized.doubleMinVal);
                Assert.Equal(values.doubleMaxVal, valuesDeserialized.doubleMaxVal);
                Assert.Equal(values.doubleNegativeInfinity, valuesDeserialized.doubleNegativeInfinity);
                Assert.Equal(values.doublePositiveInfinity, valuesDeserialized.doublePositiveInfinity);
                Assert.Equal(values.doubleNan, valuesDeserialized.doubleNan);
                Assert.Equal(values.doubleEpsilon, valuesDeserialized.doubleEpsilon);
                Assert.Equal(values.floatMinVal, valuesDeserialized.floatMinVal);
                Assert.Equal(values.floatMaxVal, valuesDeserialized.floatMaxVal);
                Assert.Equal(values.floatMaxPercision, valuesDeserialized.floatMaxPercision);
                Assert.Equal(values.floatNegativeInfinity, valuesDeserialized.floatNegativeInfinity);
                Assert.Equal(values.floatPositiveInfinity, valuesDeserialized.floatPositiveInfinity);
                Assert.Equal(values.floatNan, valuesDeserialized.floatNan);
                Assert.Equal(values.uintMaxVal, valuesDeserialized.uintMaxVal);
                Assert.Equal(values.ulongMaxVal, valuesDeserialized.ulongMaxVal);
                Assert.Equal(values.stringMaxLength, valuesDeserialized.stringMaxLength);
                Assert.Equal(values.dateMaxPercision, valuesDeserialized.dateMaxPercision);
                Assert.Equal(values.dateTimeOffsetMinVal, valuesDeserialized.dateTimeOffsetMinVal);
                Assert.Equal(values.dateTimeOffsetMaxVal, valuesDeserialized.dateTimeOffsetMaxVal);
                Assert.Equal(values.timeSpanMinVal, valuesDeserialized.timeSpanMinVal);
                Assert.Equal(values.timeSpanMaxVal, valuesDeserialized.timeSpanMaxVal);
                Assert.Equal(values.timeSpanDays, valuesDeserialized.timeSpanDays);
                Assert.Equal(values.timeSpanHours, valuesDeserialized.timeSpanHours);
                Assert.Equal(values.timeSpanMinutes, valuesDeserialized.timeSpanMinutes);
                Assert.Equal(values.timeSpanSeconds, valuesDeserialized.timeSpanSeconds);
                Assert.Equal(values.timeSpanMiliseconds, valuesDeserialized.timeSpanMiliseconds);
                Assert.Equal(values.timeSpanNanoseconds, valuesDeserialized.timeSpanNanoseconds);
                Assert.True(values.stringArray.SequenceEqual(valuesDeserialized.stringArray));
            }
        }
    }
}
