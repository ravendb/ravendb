//-----------------------------------------------------------------------
// <copyright file="InMemoryOnly.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class InMemoryOnly : RavenTest
	{
		[Fact]
		public void InMemoryDoesNotCreateDataDir()
		{
			IOExtensions.DeleteDirectory("Data");

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
			using (NewDocumentStore(runInMemory: true, port: 8079))
			{
				Assert.False(Directory.Exists("Data"));
			}
		}
	}
}