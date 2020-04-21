using System.Globalization;
using FastTests;
using Sparrow.Json;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Blittable.BlittableJsonWriterTests
{
    public class ManualBuilderTestsSlow : NoDisposalNeeded
    {
        public ManualBuilderTestsSlow(ITestOutputHelper output) : base(output)
        {
        }


        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue + 1)]
        public void BigAmountOfProperties(int propertiesAmount)
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();

                    for (int i = 0; i < propertiesAmount; i++)
                    {
                        builder.WritePropertyName("Age" + i);
                        builder.WriteValue(i);
                    }

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    using (var reader = builder.CreateReader())
                    {
                        Assert.Equal(propertiesAmount, reader.Count);
                        for (var i = 0; i < propertiesAmount; i++)
                        {
                            var val = reader["Age" + i];
                            Assert.Equal(i, int.Parse(val.ToString(), CultureInfo.InvariantCulture));
                        }
                    }
                }
            }
        }


    }
}
