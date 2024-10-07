using FastTests;
using Xunit;
using Xunit.Abstractions;
using Voron.Impl.FreeSpace;
using Tests.Infrastructure;
using Random = System.Random;

namespace SlowTests.Voron
{
    public class StreamBitArrayTests : NoDisposalNeeded
    {
        public StreamBitArrayTests(ITestOutputHelper output) : base(output)
        {
        }
   
        [RavenFact(RavenTestCategory.Voron)]
        public void VerifyResultWithRandomInput()
        {
            var random = new Random();
            var sba = new StreamBitArray();

            for (int j = 0; j < 2048; j += 1)
            {
                sba.Set(j, random.Next(2) == 1);
            }

            for (int j = 1; j <= 2048; j += 1)
            {
                var result1 = sba.GetContinuousRangeStart(j);
                var result2 = sba.GetContinuousRangeStartLegacy(j);
                Assert.Equal(result1, result2);
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1, 2)]
        [InlineData(2, 32)]
        [InlineData(32, 300)]
        [InlineData(500, 600)]
        public void VerifyResultWithRandomMinMaxInput(int minContinuous, int maxContinuous)
        {
            var random = new Random();
            var sba = new StreamBitArray();

            int i = 0;
            while (i < 2048)
            {
                int blockSize = random.Next(minContinuous, maxContinuous + 1);

                bool value = random.Next(2) == 1;

                for (int k = 0; k < blockSize && i < 2048; k++, i++)
                {
                    sba.Set(i, value);
                }
            }

            for (int j = 1; j <= 2048; j += 1)
            {
                var result1 = sba.GetContinuousRangeStart(j);
                var result2 = sba.GetContinuousRangeStartLegacy(j);
                Assert.Equal(result1, result2);
            }
        }
    }
}
