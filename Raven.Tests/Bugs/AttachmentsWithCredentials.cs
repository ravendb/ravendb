//-----------------------------------------------------------------------
// <copyright file="AttachmentsWithCredentials.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class AttachmentsWithCredentials : RavenTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}

		[Fact]
		public void CanPutAndGetAttachmentWithAccessModeNone()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] {1, 2, 3, 4}), new RavenJObject());
				Assert.Equal(new byte[] {1, 2, 3, 4}, store.DatabaseCommands.GetAttachment("ayende").Data().ReadData());
			}
		}

		[Fact]
		public void CanDeleteAttachmentWithAccessModeNone()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] {1, 2, 3, 4}), new RavenJObject());
				Assert.Equal(new byte[] {1, 2, 3, 4}, store.DatabaseCommands.GetAttachment("ayende").Data().ReadData());

				store.DatabaseCommands.DeleteAttachment("ayende", null);
				Assert.Null(store.DatabaseCommands.GetAttachment("ayende"));
			}
		}
	}
}