using System;
using System.IO;
using Raven.Database.Server.RavenFS.Util;
using Xunit;

namespace RavenFS.Tests
{
    public class NarrowedStreamTests
    {
        [Fact]
        public void Check_reading_from_begining()
        {
            var ms = PrepareSourceStream();
            var tested = new NarrowedStream(ms, 0, 5);

            var reader = new StreamReader(tested);
            var result = reader.ReadToEnd();
            Assert.Equal("000001", result);
        }

        [Fact]
        public void Check_reading_from_some_offset()
        {
            var ms = PrepareSourceStream();
            var tested = new NarrowedStream(ms, 1, 6);

            var reader = new StreamReader(tested);
            var result = reader.ReadToEnd();
            Assert.Equal("000010", result);
        }

        [Fact]
        public void Check_reading_all()
        {
            var ms = PrepareSourceStream();
            var tested = new NarrowedStream(ms, 0, ms.Length - 1);

            var reader = new StreamReader(tested);
            var result = reader.ReadToEnd();
            Assert.Equal(500000 * 6, result.Length);
            Assert.Equal(500000 * 6, tested.Length);
        }

        [Fact]
        public void Check_copy_async()
        {
            var ms = PrepareSourceStream();
            var tested = new NarrowedStream(ms, 0, ms.Length - 1);

            var reader = new StreamReader(tested);
            var result = new MemoryStream();
            tested.CopyToAsync(result).Wait();
            Assert.Equal(500000*6, result.Length);
        }

		[Fact]
		public void Should_throw_parameters_greater_than_source_length()
		{
			var ms = new MemoryStream(new byte[] { 1, 2, 3, 4 });
			ArgumentOutOfRangeException exceptionFrom = null;
			ArgumentOutOfRangeException exceptionTo = null;

			try
			{
				var tested = new NarrowedStream(ms, 10, 20);
			}
			catch (ArgumentOutOfRangeException ex)
			{
				exceptionFrom = ex;
			}

			try
			{
				var tested = new NarrowedStream(ms, 0, 20);
			}
			catch (ArgumentOutOfRangeException ex)
			{
				exceptionTo = ex;
			}
			
			Assert.NotNull(exceptionFrom);
			Assert.NotNull(exceptionTo);
			Assert.Contains("from", exceptionFrom.Message);
			Assert.Contains("to", exceptionTo.Message);
		}

        private static MemoryStream PrepareSourceStream()
        {
            var ms = new MemoryStream();
            var writer = new StreamWriter(ms);
            for (var i = 1; i <= 500000; i++)
            {
                writer.Write(i.ToString("D6"));
            }
            writer.Flush();
            return ms;
        }
    }
}
