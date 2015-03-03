// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3268_ConcurrencyChecks.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_3268_ConcurrencyChecks : RavenFilesTestWithLogs
	{
		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task CanUseOptimisticConcurrency_MetadataUpdate(string storage)
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
						var file1 = await otherSession.LoadFileAsync("test.file");

						file1.Metadata.Add("New", "Record");

						await otherSession.SaveChangesAsync();
					}

					var file2 = await session.LoadFileAsync("test.file");

					file2.Metadata.Add("New2", "Record2");

					try
					{
						await session.SaveChangesAsync();

						Assert.False(true, "Expected to throw ConcurrencyException while it didn't throw it");
					}
					catch (ConcurrencyException ex)
					{
						Assert.Equal("PUT attempted on file 'test.file' using a non current etag", ex.Message);
					}	
				}
			}
		}
	}
}