// -----------------------------------------------------------------------
//  <copyright file="EncryptedFileSystemBackupRestore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.FileSystem;
using Raven.Database.Extensions;
using Raven.Tests.Common.Util;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit.Extensions;
using Xunit;

namespace Raven.Tests.FileSystem.Encryption
{
	public class EncryptedFileSystemBackupRestore : FileSystemEncryptionTest
	{
        private readonly string backupDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "BackupRestoreTests.Backup");

		public EncryptedFileSystemBackupRestore()
        {
            IOExtensions.DeleteDirectory(backupDir);
        }

        public override void Dispose()
        {
            IOExtensions.DeleteDirectory(backupDir);
            base.Dispose();
        }

		[Theory]
		[PropertyData("Storages")]
		public async Task CanRestoreBackupIfEncryptionEnabled(string requestedStorage)
		{
			using (var client = NewAsyncClient(requestedStorage))
			{
				var server = GetServer();

				string filesystemDir = Path.Combine(server.Configuration.FileSystem.DataDirectory, "NewFS");

				await CreateSampleData(client);
				// fetch md5 sums for later verification
				var md5Sums = FetchMd5Sums(client);

				// create backup
				await client.Admin.StartBackup(backupDir, null, false, client.FileSystem);
				WaitForBackup(client, true);

				// restore newly created backup
				await client.Admin.StartRestore(new FilesystemRestoreRequest
				{
					BackupLocation = backupDir,
					FilesystemName = "NewFS",
					FilesystemLocation = filesystemDir
				});

				SpinWait.SpinUntil(() => client.Admin.GetNamesAsync().Result.Contains("NewFS"),
							Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(1));

				var restoredMd5Sums = FetchMd5Sums(client.ForFileSystem("NewFS"));
				Assert.Equal(md5Sums, restoredMd5Sums);

				var restoredClientComputedMd5Sums = ComputeMd5Sums(client.ForFileSystem("NewFS"));
				Assert.Equal(md5Sums, restoredClientComputedMd5Sums);
			}

			AssertPlainTextIsNotSavedInFileSystem("Secret", "records");
		}

		private async Task CreateSampleData(IAsyncFilesCommands commands, int startIndex = 1, int count = 2)
		{
			for (var i = startIndex; i < startIndex + count; i++)
			{
				await commands.UploadAsync(string.Format("file{0}.bin", i), StringToStream("Secret records / " + i));
			}
		}

		private string[] FetchMd5Sums(IAsyncFilesCommands filesCommands, int filesCount = 2)
		{
			return Enumerable.Range(1, filesCount).Select(i =>
			{
				var meta = filesCommands.GetMetadataForAsync(string.Format("file{0}.bin", i)).Result;
				return meta.Value<string>("Content-MD5");
			}).ToArray();
		}

		private string[] ComputeMd5Sums(IAsyncFilesCommands filesCommands, int filesCount = 2)
		{
			return Enumerable.Range(1, filesCount).Select(i =>
			{
				using (var stream = filesCommands.DownloadAsync(string.Format("file{0}.bin", i)).Result)
				{
					return stream.GetMD5Hash();
				}
			}).ToArray();
		}
	}
}