//-----------------------------------------------------------------------
// <copyright file="AttachmentsWithCredentials.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class AttachmentsWithCredentials : RemoteClientTest, IDisposable
	{
		private readonly string path;

		#region IDisposable Members

		public void Dispose()
		{
			server.Dispose();
			store.Dispose();
			IOExtensions.DeleteDirectory(path);
		}

		#endregion


	  private readonly IDocumentStore store;
		private readonly RavenDbServer server;

		public AttachmentsWithCredentials()
		{
		    path = GetPath(DbName);
			server = GetNewServerWithoutAnonymousAccess(8079, path);

			store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize();
		}


		[Fact]
		public void CanPutAndGetAttachmentWithAccessModeNone()
		{
			store.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());
			var item = store.DatabaseCommands.GetAttachment("ayende");
			Assert.Equal(new byte[] {1, 2, 3, 4}, store.DatabaseCommands.GetAttachment("ayende").Data().ReadData());
		}

		[Fact]
		public void CanDeleteAttachmentWithAccessModeNone()
		{
			store.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());

			Assert.Equal(new byte[] { 1, 2, 3, 4 }, store.DatabaseCommands.GetAttachment("ayende").Data().ReadData());

			store.DatabaseCommands.DeleteAttachment("ayende", null);

			Assert.Null(store.DatabaseCommands.GetAttachment("ayende"));
		}
	}
}