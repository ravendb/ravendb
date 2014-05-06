// -----------------------------------------------------------------------
//  <copyright file="AttachmentReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Core.Replication
{
	public class AttachmentReplication : RavenReplicationCoreTest
	{
		[Fact]
		public void CanReplicateAttachments()
		{
			using (var source = GetDocumentStore())
			using (var destination = GetDocumentStore())
			{
				SetupReplication(source, destinations: destination);

				source.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

				var attachment = WaitForAttachment(destination, "attach/1");

				Assert.NotNull(attachment);
				Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());

				source.DatabaseCommands.PutAttachment("attach/2", null, new MemoryStream(new byte[1024]), new RavenJObject());

				attachment = WaitForAttachment(destination, "attach/2");

				Assert.NotNull(attachment);
				Assert.Equal(1024, attachment.Data().ReadData().Length);
			}
		}

		[Fact]
		public void CanReplicateAttachmentDeletion()
		{
			using (var source = GetDocumentStore())
			using (var destination = GetDocumentStore())
			{
				SetupReplication(source, destinations: destination);

				source.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

				var attachment = WaitForAttachment(destination, "attach/1");

				Assert.NotNull(attachment);

				source.DatabaseCommands.DeleteAttachment("attach/1", null);

				for (int i = 0; i < RetriesCount; i++)
				{
					if (destination.DatabaseCommands.GetAttachment("attach/1") == null)
						break;
					Thread.Sleep(100);
				}

				Assert.Null(destination.DatabaseCommands.GetAttachment("attach/1"));
			}
		}

		[Fact]
		public void ShouldCreateConflictThenResolveIt()
		{
			using (var source = GetDocumentStore())
			using (var destination = GetDocumentStore())
			{
				source.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
				destination.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 3, 2, 1 }), new RavenJObject());

				SetupReplication(source, destinations: destination);

				source.DatabaseCommands.PutAttachment("marker", null, new MemoryStream(new byte[]{}), new RavenJObject());

				var marker = WaitForAttachment(destination, "marker");

				Assert.NotNull(marker);

				var conflictException = Assert.Throws<ConflictException>(() => destination.DatabaseCommands.GetAttachment("attach/1"));

				Assert.Equal("Conflict detected on attach/1, conflict must be resolved before the attachment will be accessible", conflictException.Message);

				Assert.True(conflictException.ConflictedVersionIds[0].StartsWith("attach/1/conflicts/"));
				Assert.True(conflictException.ConflictedVersionIds[1].StartsWith("attach/1/conflicts/"));

				// resolve by using first

				var resolution = destination.DatabaseCommands.GetAttachment(conflictException.ConflictedVersionIds[0]);

				destination.DatabaseCommands.PutAttachment("attach/1", null, resolution.Data(), resolution.Metadata);

				Assert.DoesNotThrow(() => destination.DatabaseCommands.GetAttachment("attach/1"));
			}
		}
	}
}