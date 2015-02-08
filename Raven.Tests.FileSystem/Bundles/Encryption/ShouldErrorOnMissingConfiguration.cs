// -----------------------------------------------------------------------
//  <copyright file="FileSystemEncryptionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.FileSystem.Bundles.Encryption
{
	public class ShouldErrorOnMissingConfiguration : RavenFilesTestBase
	{
		[Fact]
		public void ShouldThrow()
		{
			var client = NewAsyncClient();

			// secured setting nor specified
			var exception = Assert.Throws<AggregateException>(() => client.Admin.CreateFileSystemAsync(new FileSystemDocument()
			{
				Settings =
				{
					{
						Constants.ActiveBundles, "Encryption"
					}
				},
				// SecuredSettings = new Dictionary<string, string>() - intentionally not saving them - should avoid NRE on server side 
			}, "NewFS").Wait());

			Assert.Equal("Failed to create 'NewFS' file system, because of invalid encryption configuration.", exception.InnerException.Message);

			// missing Constants.EncryptionKeySetting and Constants.AlgorithmTypeSetting
			exception = Assert.Throws<AggregateException>(() => client.Admin.CreateFileSystemAsync(new FileSystemDocument()
			{
				Settings =
				{
					{
						Constants.ActiveBundles, "Encryption"
					}
				},
				SecuredSettings = new Dictionary<string, string>()
			}, "NewFS").Wait());

			Assert.Equal("Failed to create 'NewFS' file system, because of invalid encryption configuration.", exception.InnerException.Message);

			// missing Constants.EncryptionKeySetting
			exception = Assert.Throws<AggregateException>(() => client.Admin.CreateFileSystemAsync(new FileSystemDocument()
			{
				Settings =
				{
					{
						Constants.ActiveBundles, "Encryption"
					}
				},
				SecuredSettings = new Dictionary<string, string>()
				{
					{Constants.EncryptionKeySetting, ""}
				}
			}, "NewFS").Wait());

			Assert.Equal("Failed to create 'NewFS' file system, because of invalid encryption configuration.", exception.InnerException.Message);

			// missing
			exception = Assert.Throws<AggregateException>(() => client.Admin.CreateFileSystemAsync(new FileSystemDocument()
			{
				Settings =
				{
					{
						Constants.ActiveBundles, "Encryption"
					}
				},
				SecuredSettings = new Dictionary<string, string>()
				{
					{Constants.AlgorithmTypeSetting, ""}
				}
			}, "NewFS").Wait());

			Assert.Equal("Failed to create 'NewFS' file system, because of invalid encryption configuration.", exception.InnerException.Message);
		}
	}
}