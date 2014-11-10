using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem
{
    public class BigFileHandling : RavenFilesWebApiTest
	{
		[Theory]
        [InlineData(1024 * 1024)]		// 1 mb        
        [InlineData(1024 * 1024 * 8)]	// 8 mb
        public async void CanHandleBigFiles(int size)
		{
            var client = NewAsyncClient(1);
            await client.UploadAsync("mb.bin", new RandomStream(size));

            var files = await client.BrowseAsync();
            Assert.Equal(1, files.Count());
            Assert.Equal(size, files.First().TotalSize);

            var downloadData = new MemoryStream();            
            (await client.DownloadAsync("mb.bin")).CopyTo(downloadData);

            Assert.Equal(size, downloadData.Length);
		}

		[Theory]
		[SizeAndPartition(BaseSize = 1024*1024*2, Sizes = 2, Partitions = 3)]
		public async void CanReadPartialFiles(int size, int skip)
		{
            var buffer = new byte[size];
            new Random().NextBytes(buffer);

            var client = NewAsyncClient(1);
            await client.UploadAsync("mb.bin", new MemoryStream(buffer));

            var files = await client.BrowseAsync();
            Assert.Equal(1, files.Count());
            Assert.Equal(size, files.First().TotalSize);

            var downloadData = new MemoryStream();
            (await client.DownloadAsync("mb.bin", null, skip)).CopyTo(downloadData);

            var expected = buffer.Skip(skip).ToArray();
            Assert.Equal(expected.Length, downloadData.Length);
            Assert.True(expected.SequenceEqual(downloadData.ToArray()));
		}

		public class SizeAndPartition : DataAttribute
		{
			public int BaseSize { get; set; }
			public int Sizes { get; set; }
			public int Partitions { get; set; }


			public override IEnumerable<object[]> GetData(MethodInfo methodUnderTest, Type[] parameterTypes)
			{
				for (var i = 0; i < Sizes; i++)
				{
					for (var j = 0; j < Partitions; j++)
					{
						var currentSize = (i+1)*BaseSize;
						yield return new object[] {currentSize, currentSize/(j + 1)};
					}
				}
			}
		}
	}
}