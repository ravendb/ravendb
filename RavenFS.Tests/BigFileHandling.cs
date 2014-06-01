using System;
using System.Collections.Generic;
using System.Reflection;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using RavenFS.Tests.Tools;
using Xunit;
using Xunit.Extensions;
using System.Linq;
using Raven.Json.Linq;
using RavenFS.Tests.Synchronization.IO;
using System.IO;

namespace RavenFS.Tests
{
    public class BigFileHandling : RavenFsWebApiTest
	{
		[Theory]
        [InlineData(1024 * 1024)]		// 1 mb        
        [InlineData(1024 * 1024 * 8)]	// 8 mb
        public async void CanHandleBigFiles(int size)
		{
            var client = NewClient(1);
            await client.UploadAsync("mb.bin", new RandomStream(size));

            var files = await client.BrowseAsync();
            Assert.Equal(1, files.Count());
            Assert.Equal(size, files.First().TotalSize);

            // REVIEW: (Oren) what does UploadedSize means and where it is sent over? RavenFS-Size is that?
            //Assert.Equal(size, files.First().UploadedSize);

            var downloadData = new MemoryStream();
            await client.DownloadAsync("mb.bin", downloadData);

            Assert.Equal(size, downloadData.Length);
		}

		[Theory]
		[SizeAndPartition(BaseSize = 1024*1024*2, Sizes = 2, Partitions = 3)]
		public async void CanReadPartialFiles(int size, int skip)
		{
            var buffer = new byte[size];
            new Random().NextBytes(buffer);

            var client = NewClient(1);
            await client.UploadAsync("mb.bin", new MemoryStream(buffer));

            var files = await client.BrowseAsync();
            Assert.Equal(1, files.Count());
            Assert.Equal(size, files.First().TotalSize);

            var downloadData = new MemoryStream();
            await client.DownloadAsync("mb.bin", downloadData, skip);

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