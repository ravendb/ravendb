// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3268_ConcurrencyChecks.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Database.Extensions;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_3268_ConcurrencyChecks : RavenFilesTestBase
	{
		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task CanEnableUseOptimisticConcurrency_ShouldThrowOnMetadataUpdate(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);
					session.Advanced.UseOptimisticConcurrency = true;

					session.RegisterUpload("test.file", new MemoryStream());

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						var file2 = await otherSession.LoadFileAsync("test.file");

						file2.Metadata.Add("New", "Record");

						await otherSession.SaveChangesAsync();
					}

					var file = await session.LoadFileAsync("test.file");

					file.Metadata.Add("New2", "Record2");

					try
					{
						await session.SaveChangesAsync();

						Assert.False(true, "Expected to throw ConcurrencyException while it didn't throw it");
					}
					catch (ConcurrencyException ex)
					{
						Assert.Equal("Operation attempted on file '/test.file' using a non current etag", ex.Message);

						Assert.NotNull(ex.ExpectedETag);
						Assert.NotNull(ex.ActualETag);
					}	
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task OptimisticConcurrencyDisabledByDefault_ShouldNotThrowOnMetadataUpdate(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					session.RegisterUpload("test.file", new MemoryStream());

					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);

					var file = await session.LoadFileAsync("test.file");

					using (var otherSession = store.OpenAsyncSession())
					{
						var file2 = await otherSession.LoadFileAsync("test.file");

						file2.Metadata.Add("New", "Record");

						await otherSession.SaveChangesAsync();
					}

					file.Metadata.Add("New2", "Record2");

					try
					{
						await session.SaveChangesAsync();
					}
					catch (Exception ex)
					{
						Assert.False(true, "It shouldn't throw the following exception: " + ex.Message);
					}

					var metadata = await session.Commands.GetMetadataForAsync("test.file");

					Assert.Equal("Record2", metadata["New2"]);
					Assert.DoesNotContain("New", metadata.Keys);
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task CanEnableUseOptimisticConcurrency_ShouldThrowOnContentUpdate(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);
					session.Advanced.UseOptimisticConcurrency = true;

					session.RegisterUpload("test.file", new MemoryStream());

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						session.RegisterUpload("test.file", new MemoryStream());

						await otherSession.SaveChangesAsync();
					}

					var file = await session.LoadFileAsync("test.file");

					session.RegisterUpload(file, new MemoryStream());

					try
					{
						await session.SaveChangesAsync();

						Assert.False(true, "Expected to throw ConcurrencyException while it didn't throw it");
					}
					catch (ConcurrencyException ex)
					{
						Assert.Equal("Operation attempted on file '/test.file' using a non current etag", ex.Message);

						Assert.NotNull(ex.ExpectedETag);
						Assert.NotNull(ex.ActualETag);
					}
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task OptimisticConcurrencyDisabledByDefault_ShouldNotThrowOnContentUpdate(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);

					session.RegisterUpload("test.file", CreateUniformFileStream(128, 'x'));

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						session.RegisterUpload("test.file", CreateUniformFileStream(128, 'y'));

						await otherSession.SaveChangesAsync();
					}

					var file = await session.LoadFileAsync("test.file");

					session.RegisterUpload(file, CreateUniformFileStream(128, 'z'));

					try
					{
						await session.SaveChangesAsync();
					}
					catch (Exception ex)
					{
						Assert.False(true, "It shouldn't throw the following exception: " + ex.Message);
					}

					var fileContent = await session.Commands.DownloadAsync("test.file");

					Assert.Equal(CreateUniformFileStream(128, 'z').GetMD5Hash(), fileContent.GetMD5Hash());
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task CanEnableUseOptimisticConcurrency_ShouldThrowOnRename(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);
					session.Advanced.UseOptimisticConcurrency = true;

					session.RegisterUpload("test.file", new MemoryStream());

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						var file = await otherSession.LoadFileAsync("test.file");

						file.Metadata.Add("file", "changed");

						await otherSession.SaveChangesAsync();
					}

					session.RegisterRename("test.file", "new.file");

					try
					{
						await session.SaveChangesAsync();

						Assert.False(true, "Expected to throw ConcurrencyException while it didn't throw it");
					}
					catch (ConcurrencyException ex)
					{
						Assert.Equal("Operation attempted on file '/test.file' using a non current etag", ex.Message);

						Assert.NotNull(ex.ExpectedETag);
						Assert.NotNull(ex.ActualETag);
					}
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task OptimisticConcurrencyDisabledByDefault_ShouldNotThrowOnRename(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);

					session.RegisterUpload("test.file", CreateUniformFileStream(1));

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						session.RegisterUpload("test.file", CreateUniformFileStream(128, 'y'));

						await otherSession.SaveChangesAsync();
					}

					var file = await session.LoadFileAsync("test.file");

					session.RegisterRename(file, "new.file");

					try
					{
						await session.SaveChangesAsync();
					}
					catch (Exception ex)
					{
						Assert.False(true, "It shouldn't throw the following exception: " + ex.Message);
					}

					var newFile = await session.Commands.GetMetadataForAsync("new.file");

					Assert.NotNull(newFile);
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task CanEnableUseOptimisticConcurrency_ShouldThrowOnDelete(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);
					session.Advanced.UseOptimisticConcurrency = true;

					session.RegisterUpload("test.file", new MemoryStream());

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						var file = await otherSession.LoadFileAsync("test.file");

						file.Metadata.Add("file", "changed");

						await otherSession.SaveChangesAsync();
					}

					session.RegisterFileDeletion("test.file");

					try
					{
						await session.SaveChangesAsync();

						Assert.False(true, "Expected to throw ConcurrencyException while it didn't throw it");
					}
					catch (ConcurrencyException ex)
					{
						Assert.Equal("Operation attempted on file '/test.file' using a non current etag", ex.Message);

						Assert.NotNull(ex.ExpectedETag);
						Assert.NotNull(ex.ActualETag);
					}
				}
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task OptimisticConcurrencyDisabledByDefault_ShouldNotThrowOnDelete(string storage)
		{
			using (var store = NewStore(requestedStorage: storage))
			{
				using (var session = store.OpenAsyncSession())
				{
					Assert.False(session.Advanced.UseOptimisticConcurrency);

					session.RegisterUpload("test.file", new MemoryStream());

					await session.SaveChangesAsync();

					using (var otherSession = store.OpenAsyncSession())
					{
						var file = await otherSession.LoadFileAsync("test.file");

						file.Metadata.Add("file", "changed");

						await otherSession.SaveChangesAsync();
					}

					session.RegisterFileDeletion("test.file");

					try
					{
						await session.SaveChangesAsync();
					}
					catch (Exception ex)
					{
						Assert.False(true, "It shouldn't throw the following exception: " + ex.Message);
					}

					var deletedFile = await session.Commands.GetMetadataForAsync("test.file");

					Assert.Null(deletedFile);
				}
			}
		}
	}
}