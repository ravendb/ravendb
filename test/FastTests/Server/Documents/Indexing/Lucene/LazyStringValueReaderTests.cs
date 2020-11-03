using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Indexing.Lucene
{
    public class LazyStringValueReaderTests : RavenLowLevelTestBase
    {
        private readonly LazyStringReader _sut = new LazyStringReader();
        private readonly JsonOperationContext _ctx;

        public LazyStringValueReaderTests(ITestOutputHelper output) : base(output)
        {
            _ctx = JsonOperationContext.ShortTermSingleUse();
        }

        [Theory]
        [InlineData("ąęłóżźćń")]
        [InlineData("לכובע שלי שלוש פינות")]
        public void Reads_unicode(string expected)
        {
            using (var lazyString = _ctx.GetLazyString(expected))
            {
                var stringResult = LazyStringReader.GetStringFor(lazyString);
                var readerResult = _sut.GetTextReaderFor(lazyString);

                Assert.Equal(expected, stringResult);
                Assert.Equal(expected, readerResult.ReadToEnd());
            }
        }

        [Theory]
        [InlineData(1024)]
        [InlineData(1500)]
        [InlineData(2048)]
        [InlineData(3000)]
        public void Reads_very_long_text(int length)
        {
            var expected = new string('a', length);
            using (var lazyString = _ctx.GetLazyString(expected))
            {
                var stringResult = LazyStringReader.GetStringFor(lazyString);
                var readerResult = _sut.GetTextReaderFor(lazyString);

                Assert.Equal(expected, stringResult);
                Assert.Equal(expected, readerResult.ReadToEnd());
            }
        }

        [Theory]
        [InlineData(10)]
        [InlineData(1500)]
        [InlineData(2048)]
        [InlineData(3000)]
        public void First_on_very_long_text(int length)
        {
            var expected = new string('a', length);
            using var lazyString = _ctx.GetLazyString(expected);
            var stringResult = lazyString.First();

            Assert.Equal(expected.First(), stringResult);
        }

        [Fact]
        public void Can_reuse_reader_multiple_times()
        {
            var r = new Random();

            for (int i = 0; i < 10; i++)
            {
                var bytes = RandomString(2000);

                var expected = Encoding.UTF8.GetString(bytes);

                var lazyString = _ctx.GetLazyString(expected);

                var stringResult = LazyStringReader.GetStringFor(lazyString);
                var readerResult = _sut.GetTextReaderFor(lazyString);

                Assert.Equal(expected, stringResult);
                Assert.Equal(expected, readerResult.ReadToEnd());
            }
        }

        [Fact]
        public async Task CompareLazyCompressedStringValue()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            await using (var ms = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Test");
                writer.WriteString(new string('c', 1024 * 1024));
                writer.WriteEndObject();
                await writer.FlushAsync();
                await ms.FlushAsync();

                ms.Position = 0;
                var json = await context.ReadForDiskAsync(ms, "test");

                ms.Position = 0;
                var json2 = await context.ReadForDiskAsync(ms, "test");

                Assert.IsType<LazyCompressedStringValue>(json["Test"]);
                Assert.IsType<LazyCompressedStringValue>(json2["Test"]);
                Assert.Equal(json["Test"], json2["Test"]);
            }
        }

        public byte[] RandomString(int length)
        {
            Random random = new Random();
            var charLength = random.Next(1, 2000);
            var actualSize = charLength;
            StringBuilder str = new StringBuilder(length);
            while (charLength > 0)
            {
                char c = (char)random.Next(char.MinValue, char.MaxValue);
                if (c >= 0xD800 && c <= 0xDFFF || c >= 0xE000)
                    continue;
                if (c < 32)
                    c += (char)32;
                str.Append(c);
                charLength--;
            }
            return Encoding.UTF8.GetBytes(str.ToString());
        }

        public override void Dispose()
        {
            _ctx.Dispose();

            base.Dispose();
        }
    }
}
