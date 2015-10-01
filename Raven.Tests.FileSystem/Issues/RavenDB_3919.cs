// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3919.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.FileSystem;
using Raven.Tests.Common.Util;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_3919 : RavenFilesTestWithLogs
	{
		[Fact]
		public async Task after_failed_upload_file_should_not_exist()
		{
			const int fileSize = 1024*1024*2; // 2 MB
			const string fileName = "test.bin";
			const int portIndex = 0;

			using (var initialStore = NewStore(portIndex))
			{
				var alreadyReset = false;
				var port = 8070;
				var putInitialized = false;
				var uploaded = 0;

				using (new ProxyServer(ref port, Ports[portIndex])
				{
					VetoTransfer = (totalRead, buffer) =>
					{
						var numOfBytes = buffer.Count - buffer.Offset;
						var s = Encoding.UTF8.GetString(buffer.Array, buffer.Offset, buffer.Count);

						if (s.StartsWith("PUT"))
						{
							putInitialized = true;
							uploaded = numOfBytes;
							return false;
						}

						if (putInitialized && alreadyReset == false)
						{
							uploaded += numOfBytes;

							if (uploaded > fileSize / 2)
							{
								alreadyReset = true;
								return true;
							}
						}

						return false;
					}
				})
				{
					using (var storeUsingProxy = new FilesStore
					{
						Url = "http://localhost:" + port,
						DefaultFileSystem = initialStore.DefaultFileSystem
					}.Initialize())
					{
						HttpRequestException hre = null;

						try
						{
							await storeUsingProxy.AsyncFilesCommands.UploadAsync(fileName, new MemoryStream(new byte[fileSize]));
						}
						catch (HttpRequestException e)
						{
							hre = e;
						}

						Assert.NotNull(hre);

						// the exception we caught was actually the client side exception because our proxy server forced the connection to be closed
						// wait a bit to make sure the actual request completed and the file has been marked as deleted on the server side

						Thread.Sleep(1000);

						var fileMetadata = await storeUsingProxy.AsyncFilesCommands.GetMetadataForAsync(fileName);

						Assert.Null(fileMetadata);
					}
				}
			}
		}
	}
}