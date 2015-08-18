using System.Text;
using Xunit;

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

    }
}
