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

namespace RavenFS.Tests
{
    public class BigFileHandling : RavenFsWebApiTest
	{
		[Theory]
        [InlineData(1024 * 1024)]		// 1 mb        
        [InlineData(1024 * 1024 * 8)]	// 8 mb
		public void CanHandleBigFiles(int size)
		{
			var buffer = new byte[size];
			new Random().NextBytes(buffer);

            WebClient.UploadData(GetFsUrl("/files/mb.bin"), "PUT", buffer);

            var downloadString = WebClient.DownloadString(GetFsUrl("/files/"));
			var files = JsonConvert.DeserializeObject<List<FileHeader>>(downloadString, new NameValueCollectionJsonConverter());
			Assert.Equal(1, files.Count);
			Assert.Equal(buffer.Length, files[0].TotalSize);
			Assert.Equal(buffer.Length, files[0].UploadedSize);


            var downloadData = WebClient.DownloadData(GetFsUrl("/files/mb.bin"));

			Assert.Equal(buffer.Length, downloadData.Length);
			Assert.Equal(buffer, downloadData);
		}

		[Theory]
		[SizeAndPartition(BaseSize = 1024*1024*2, Sizes = 2, Partitions = 3)]
		public void CanReadPartialFiles(int size, int skip)
		{
			var buffer = new byte[size];
			new Random().NextBytes(buffer);

            WebClient.UploadData(GetFsUrl("/files/mb.bin"), "PUT", buffer);

            var files = JsonConvert.DeserializeObject<List<FileHeader>>(WebClient.DownloadString(GetFsUrl("/files/")),
			                                                            new NameValueCollectionJsonConverter());
			Assert.Equal(1, files.Count);
			Assert.Equal(buffer.Length, files[0].TotalSize);
			Assert.Equal(buffer.Length, files[0].UploadedSize);

            var readData = CreateWebRequest(GetFsUrl("/files/mb.bin"))
				.WithRange(skip)
				.MakeRequest()
				.ReadData();

			var expected = buffer.Skip(skip).ToArray();
			Assert.Equal(expected.Length, readData.Length);

			Assert.True(expected.SequenceEqual(readData));
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