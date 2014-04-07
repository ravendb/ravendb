// -----------------------------------------------------------------------
//  <copyright file="DocumentRetrieving.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Commands
{
	public class DocumentsRetrieving : RavenCoreTestBase
	{
		[Fact]
		public async Task CanGetDocumentsWhoseIdStartsWithAPrefix()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new Company { Name = "Something with the desired prefix" });
					await session.StoreAsync(new Contact { Surname = "Something without the desired prefix" });
					await session.SaveChangesAsync();
				}

				var documents = await store.AsyncDatabaseCommands.StartsWithAsync("Companies", null, 0, 25);
				Assert.Equal(1, documents.Length);
			}
		}
	}
}