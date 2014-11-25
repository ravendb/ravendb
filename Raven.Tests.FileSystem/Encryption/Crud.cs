// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.FileSystem.Encryption
{
	public class Crud : FileSystemEncryptionTest
	{
		[Fact]
		public async Task ShouldEncryptData()
		{
			var client = NewAsyncClient();

			await client.UploadAsync("test.txt", StringToStream("Lorem ipsum"));

			AssertPlainTextIsNotSavedInFileSystem("test.txt", "Lorem", "ipsum");
		}
	}
}