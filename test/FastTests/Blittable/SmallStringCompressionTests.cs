// -----------------------------------------------------------------------
//  <copyright file="SmallStringCompression.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Sparrow.Compression;
using Xunit;

namespace FastTests.Blittable
{
    public unsafe class SmallStringCompressionTests : NoDisposalNeeded
    {
        [Theory]
        [InlineData("this is a sample string")]
        [InlineData("here is a funny story I have heard")]
        [InlineData("who is here, and who is there, who is everywhere?")]
        [InlineData("https://ravendb.net")]
        [InlineData("noreply@example.com")]
        [InlineData("See: here")]
        [InlineData("בארזים נפלה שלהבת")]
        public void RoundTrip(string s)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            var buffer = new byte[128];
            var results = new byte[128];
            fixed (byte* input = bytes)
            {
                fixed (byte* final = results)
                {
                    fixed (byte* output = buffer)
                    {
                        var size = SmallStringCompression.Instance.Compress(input, output, bytes.Length, buffer.Length);
                        size = SmallStringCompression.Instance.Decompress(output, size, final, results.Length);
                        var actual = Encoding.UTF8.GetString(results, 0, size);
                        Assert.Equal(s, actual);
                    }
                }
            }
        }

        [Theory]
        [InlineData("this is a sample string")]
        [InlineData("here is a funny story I have heard")]
        [InlineData("who is here, and who is there, who is everywhere?")]
        [InlineData("https://ravendb.net")]
        [InlineData("noreply@example.com")]
        [InlineData("בארזים נפלה שלהבת")]
        public void CanHandleSmallBufferDecompression(string s)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            var buffer = new byte[128];
            var results = new byte[128];
            results[4] = 111;
            fixed (byte* input = bytes)
            {
                fixed (byte* final = results)
                {
                    fixed (byte* output = buffer)
                    {
                        var size = SmallStringCompression.Instance.Compress(input, output, bytes.Length, buffer.Length);
                        size = SmallStringCompression.Instance.Decompress(output, size, final, 4);
                        Assert.Equal(0, size);
                        Assert.Equal(111, results[4]);
                    }
                }
            }
        }


        [Theory]
        [InlineData("this is a sample string")]
        [InlineData("בארזים נפלה שלהבת")]
        public void CanHandleSmallBufferCompression(string s)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            var buffer = new byte[128];
            buffer[4] = 111;
            fixed (byte* input = bytes)
            {
                fixed (byte* output = buffer)
                {
                    var size = SmallStringCompression.Instance.Compress(input, output, bytes.Length, 4);
                    Assert.Equal(0, size);
                    Assert.Equal(111, buffer[4]);
                }
            }
        }

        [Theory]
        [InlineData("this is a sample string")]
        [InlineData("here is a funny story I have heard")]
        [InlineData("who is here, and who is there, who is everywhere?")]
        [InlineData("https://ravendb.net")]
        [InlineData("noreply@example.com")]
        public void SmallerForThoseValues(string s)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            var buffer = new byte[128];
            fixed (byte* input = bytes)
            {
                fixed (byte* output = buffer)
                {
                    var size = SmallStringCompression.Instance.Compress(input, output, bytes.Length, buffer.Length);
                    Assert.True(size < bytes.Length);
                }
            }
        }
    }
}