// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3920.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_3920 : RavenFilesTestWithLogs
	{
		[Fact]
		public async Task ShouldWork()
		{
			const string fileName = "file.bin";

			using (var store = NewStore())
			{
				var rfs = GetFileSystem();

				await store.AsyncFilesCommands.UploadAsync(fileName, new MemoryStream(new byte[2 * 1024 * 1024]));

				using (var session = store.OpenAsyncSession())
				{
					session.RegisterUpload(fileName, 10, s =>
					{
						s.WriteByte(1);
						s.WriteByte(2);
						s.WriteByte(3);
					});

					await ThrowsAsync<BadRequestException>(() => session.SaveChangesAsync()); // 10 bytes declared but only 3 has been uploaded, IndicateFileToDelete is going to be called underhood
				}

				using (var session = store.OpenAsyncSession())
				{
					session.RegisterUpload(fileName, 3, s =>
					{
						s.WriteByte(1);
						s.WriteByte(2);
						s.WriteByte(3);
					});

					await session.SaveChangesAsync();
				}

				await rfs.Files.CleanupDeletedFilesAsync(); // should not delete existing file

				var downloaded = new MemoryStream();

				(await store.AsyncFilesCommands.DownloadAsync(fileName)).CopyTo(downloaded);
				
				Assert.Equal(3, downloaded.Length);
			}
		}
	}
}