// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3887.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_3887 : RavenFilesTestBase
	{
		[Theory]
		[InlineData(4194304, "esent")]
		[InlineData(4194304, "voron")]
		[InlineData(4194305, "esent")]
		[InlineData(4194305, "voron")]
		[InlineData(9 * 1024 * 1024, "esent")]
		[InlineData(9 * 1024 * 1024, "voron")]
		public async Task ShouldNotReturnCorruptedFileContent(int size, string storage)
		{
			const string path = "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789abc";

			byte[] originalFileData = new byte[size];
			byte[] originalFileHash;
			byte[] storedFileData;
			byte[] storedFileHash;

			using (var rng = RNGCryptoServiceProvider.Create())
			using (var sha1 = SHA1.Create())
			{
				rng.GetBytes(originalFileData);
				originalFileHash = sha1.ComputeHash(originalFileData);
			}

			using (var filesStore = NewStore(requestedStorage: storage))
			{
				using (var session = filesStore.OpenAsyncSession())
				{
					session.RegisterUpload(path, new MemoryStream(originalFileData));
					await session.SaveChangesAsync();
				}

				using (var session = filesStore.OpenAsyncSession())
				{
					var refMetadata = new Reference<RavenJObject>();
					var stream = await session.DownloadAsync(path, refMetadata);
					storedFileData = await stream.ReadDataAsync();

					Assert.Equal(originalFileData.Length, storedFileData.Length);

					using (var sha1 = SHA1.Create())
					{
						storedFileHash = sha1.ComputeHash(storedFileData);
					}
				}

				Assert.Equal(ToHexString(originalFileHash), ToHexString(storedFileHash));
			}
		}

		string ToHexString(byte[] data)
		{
			if (data == null) return null;

			const string hexDigits = "0123456789abcdef";
			var sb = new StringBuilder(data.Length * 2);
			foreach (byte b in data)
			{
				sb.Append(hexDigits[(b >> 4) & 0x0F]).Append(hexDigits[b & 0x0F]);
			}
			return sb.ToString();
		}
	}
}