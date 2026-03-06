using System;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Voron.Impl.FreeSpace;
using Xunit;

namespace FastTests.Voron
{
    public class StreamBitArrayTests : NoDisposalNeeded
    {
        public StreamBitArrayTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void VerifySingleSmallResult()
        {
            int[] arr =
            [
                962919237, 1548146341, 887074999, -1149229772, 1614889381, 1195459377, 241536840, 1976691873, 974616527, -1975930694, -842246417, 1992663471, -441749934,
                1493649717, -1200378937, 316033463, 1754441570, -1514685861, 2086876795, -2023483551, -850791867, 2122928129, -2028633376, -1888571066, -1887046545,
                -674291990, 937660561, 1660582309, -1011073175, -460586860, 145201398, 545934217, -1348896473, 529588057, -2125877939, 748147114, 304154730, -553437939,
                -1685030522, 65856665, -2095037481, 1700335264, 1057375282, 488608589, -968824882, -942978190, -14718, 1690458861, -1432094240, -68039965, -392179582,
                -1532304670, 695723974, -1515228467, -63736809, -271307999, 311526503, -1606718741, 2089777125, -633659368, -1900351717, 1564141405, 1909671261, 5492821
            ];

            var sba = new StreamBitArray();

            for (var wordIndex = 0; wordIndex < arr.Length; wordIndex++)
            {
                var word = arr[wordIndex];
                for (int i = 0; i < 32; i++)
                {
                    if ((word & (1 << i)) == 0)
                        continue;

                    int globalIndex = wordIndex * 32 + i;
                    sba.Set(globalIndex, true);
                }
            }

            const int num = 19;
            var result1 = GetContinuousRangeSlow(sba, num);
            var result2 = sba.GetContinuousRangeStart(num);
            Assert.Equal(result1, result2);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void VerifySingleLargeResultSearching100()
        {
            int[] arr =
            [
                0, 0, 0, 0, 0, 0, -256, -1, -1, -1, -1, -1, 65535, 0, 0, 0, 0, 0, -16777216, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, -256, -1, -1, -1, -1, -1, 65535, 0,
                0, 0, 0, 0, -16777216, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, -256, -1, -1, -1, -1, -1, 65535, 0
            ];

            var sba = new StreamBitArray();

            for (var wordIndex = 0; wordIndex < arr.Length; wordIndex++)
            {
                var word = arr[wordIndex];
                for (int i = 0; i < 32; i++)
                {
                    if ((word & (1 << i)) == 0)
                        continue;

                    int globalIndex = wordIndex * 32 + i;
                    sba.Set(globalIndex, true);
                }
            }

            const int num = 100;
            var result1 = GetContinuousRangeSlow(sba, num);
            var result2 = sba.GetContinuousRangeStart(num);
            Assert.Equal(result1, result2);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void VerifySingleLargeResultSearching451()
        {
            int[] arr =
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -8, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 8388607, 0, -1, -1, -1, -1, -1,
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
            ];

            var sba = new StreamBitArray();

            for (var wordIndex = 0; wordIndex < arr.Length; wordIndex++)
            {
                var word = arr[wordIndex];
                for (int i = 0; i < 32; i++)
                {
                    if ((word & (1 << i)) == 0)
                        continue;

                    int globalIndex = wordIndex * 32 + i;
                    sba.Set(globalIndex, true);
                }
            }

            const int num = 451;
            var result1 = GetContinuousRangeSlow(sba, num);
            var result2 = sba.GetContinuousRangeStart(num);
            Assert.Equal(result1, result2);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void VerifyResultWithRandomInput(int seed)
        {
            var random = new Random(seed);
            var sba = new StreamBitArray();

            for (int j = 0; j < 2048; j += 1)
            {
                sba.Set(j, random.Next(2) == 1);
            }

            for (int j = 1; j <= 2048; j += 1)
            {
                var result1 = GetContinuousRangeSlow(sba, j);
                var result2 = sba.GetContinuousRangeStart(j);
                Assert.Equal(result1, result2);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed(1, 2)]
        [InlineDataWithRandomSeed(2, 32)]
        [InlineDataWithRandomSeed(32, 300)]
        [InlineDataWithRandomSeed(32, 2048)]
        [InlineDataWithRandomSeed(1, 2048)]
        public void VerifyResultWithRandomMinMaxInput(int minContinuous, int maxContinuous, int seed)
        {
            var random = new Random(seed);
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
                var result1 = GetContinuousRangeSlow(sba, j);
                var result2 = sba.GetContinuousRangeStart(j);
                Assert.Equal(result1, result2);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed(0, 2)]
        [InlineDataWithRandomSeed(2, 32)]
        [InlineDataWithRandomSeed(32, 64)]
        [InlineDataWithRandomSeed(64, 2048)]
        [InlineDataWithRandomSeed(0, 2048)]
        public void VerifyEndRangeCountWithRandomMinMaxInput(int minContinuous, int maxContinuous, int seed)
        {
            var random = new Random(seed);
            var sba = new StreamBitArray();

            int i = 2047;

            while (i >= 0)
            {
                int blockSize = random.Next(minContinuous, maxContinuous + 1);
                bool value = random.Next(2) == 1;

                for (int k = 0; k < blockSize && i >= 0; k++, i--)
                {
                    sba.Set(i, value);
                }
            }

            var result1 = GetEndRangeCountSlow(sba);
            var result2 = sba.GetEndRangeCount();
            Assert.Equal(result1, result2);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed(0, 2)]
        [InlineDataWithRandomSeed(2, 32)]
        [InlineDataWithRandomSeed(32, 64)]
        [InlineDataWithRandomSeed(64, 2048)]
        [InlineDataWithRandomSeed(0, 2048)]
        public void VerifyStartRangeCountWithRandomMinMaxInput(int minContinuous, int maxContinuous, int seed)
        {
            var random = new Random(seed);
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

            var result1 = GetStartRangeCount(sba);
            var result2 = sba.GetStartRangeCount();
            Assert.Equal(result1, result2);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed(0, 2)]
        [InlineDataWithRandomSeed(2, 32)]
        [InlineDataWithRandomSeed(32, 64)]
        [InlineDataWithRandomSeed(64, 2048)]
        [InlineDataWithRandomSeed(0, 2048)]
        public void VerifyHasStartRangeCountWithRandomMinMaxInput(int minContinuous, int maxContinuous, int seed)
        {
            var random = new Random(seed);
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
                var result1 = HasStartRangeCount(sba, j);
                var result2 = sba.HasStartRangeCount(j);
                Assert.Equal(result1, result2);
            }
        }

        private static int? GetContinuousRangeSlow(StreamBitArray current, int num)
        {
            var start = -1;
            var count = 0;

            for (int i = 0; i < 2048; i++)
            {
                if (current.Get(i))
                {
                    if (start == -1)
                        start = i;
                    count++;

                    if (count == num)
                        return start;
                }
                else
                {
                    start = -1;
                    count = 0;
                }
            }

            return null;
        }

        public int GetEndRangeCountSlow(StreamBitArray current)
        {
            int c = 0;

            for (int i = 2048 - 1; i >= 0; i--)
            {
                if (current.Get(i) == false)
                    break;
                c++;
            }

            return c;
        }

        public int GetStartRangeCount(StreamBitArray current)
        {
            int c = 0;

            for (int i = 0; i < 2048; i++)
            {
                if (current.Get(i) == false)
                    break;
                c++;
            }

            return c;
        }

        public bool HasStartRangeCount(StreamBitArray current, int max)
        {
            int c = 0;

            for (int i = 0; i < 2048 && c < max; i++)
            {
                if (current.Get(i) == false)
                    break;
                c++;
            }

            return c == max;
        }
    }
}
