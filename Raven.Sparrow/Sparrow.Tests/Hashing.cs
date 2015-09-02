using System.Text;
using Xunit;
using Xunit.Extensions;

namespace Sparrow.Tests
{
    public class HashingTests
    {
        [Fact]
        public void HashRawUseActualValues()
        {
            var r1 = Hashing.XXHash64.CalculateRaw("Public");
            var r2 = Hashing.XXHash64.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);

        }

        [Fact]
        public void HashRaw32UseActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("Public");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);

        }

        [Fact]
        public void XXHash32()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            uint expected = 0xA3643705;
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0x32D153FF;
            Assert.Equal(expected, result);

            value = "heiå";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0xDB5ABCCC;
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0xD855F606;
            Assert.Equal(expected, result);

            value = "asdfasdfasdfasdfasdfasdfasdfuqrewpuqpuqruepoiqiwoerpuqowieruqwlekjrqwernq/wemnrq.,wemrn";
            var bytes = Encoding.UTF8.GetBytes(value);
            result = Hashing.XXHash32.Calculate(bytes);
            expected = 3571373779;
            Assert.Equal(expected, result);
        }

        [Fact]
        public void EquivalenceInDifferentMemoryLocations()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            uint expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);
        }


        [Fact]
        public void NotEquivalenceOfBytesWithString()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            uint expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "abc";
            result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);
        }

        public static IEnumerable<object[]> BufferSize
        {
            get
            {
                return new[]
				{
                    new object[] {1},
                    new object[] {4},
                    new object[] {15},                  
                    new object[] {65},
                    new object[] {90},
                    new object[] {128},
                    new object[] {129},
				};
            }
        }

        [Theory]
        [PropertyData("BufferSize")]
        public void IterativeHashingEquivalence(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;
            var context = Hashing.Iterative.XXHash32.Preprocess(values, values.Length, seed);

            for (int i = 1; i < values.Length; i++)
            {
                var expected = Hashing.XXHash32.Calculate(values, i, seed);
                var result = Hashing.Iterative.XXHash32.Calculate(values, i, context);
                Assert.Equal(expected, result);
            }
        }

        [Theory]
        [PropertyData("BufferSize")]
        public void IterativeHashingPrefixing(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;
            var context = Hashing.Iterative.XXHash32.Preprocess(values, values.Length, seed);

            for (int i = 1; i < values.Length; i++)
            {
                var expected = Hashing.XXHash32.Calculate(values, i, seed);

                for (int j = 0; j < i; j++)
                {
                    var result = Hashing.Iterative.XXHash32.Calculate(values, i, context, j);
                    Assert.Equal(expected, result);
                }
            }
        }

        [Fact]
        public void Combine()
        {
           int h1 = Hashing.CombineInline(1991, 13);
           int h2 = Hashing.CombineInline(1991, 12);
           Assert.NotEqual(h1, h2);
        }

    }
}
