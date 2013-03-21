//-----------------------------------------------------------------------
// <copyright file="InMemoryOnly.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class InMemoryOnly
	{
		[Fact]
		public void InMemoryDoesNotCreateDataDir()
		{
			IOExtensions.DeleteDirectory("Data");

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				UseEmbeddedHttpServer = true,
				Configuration = 
				{
					Port = 8079,
					RunInMemory = true
				}
			})
			{
				store.Initialize();

				Assert.False(Directory.Exists("Data"));
			}
		}
	}
}