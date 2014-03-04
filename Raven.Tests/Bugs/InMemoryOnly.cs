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
	public class InMemoryOnly : RavenTest
	{
		[TimeBombedFact(2014, 3, 15, "Waiting for RavenDB-1611 issue to be fixed")]
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