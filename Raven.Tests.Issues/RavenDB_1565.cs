// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1565.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1565 : ReplicationBase
	{
		public class Item
		{
			public string Name { get; set; } 
		}

		[Fact]
		public void ShouldResolveDocumentConflictInFavorOfLocalVersion()
		{
			DocumentConflictResolveTest(StraightforwardConflictResolution.ResolveToLocal);
		}

		[Fact]
		public void ShouldResolveDocumentConflictInFavorOfRemoteVersion()
		{
			DocumentConflictResolveTest(StraightforwardConflictResolution.ResolveToRemote);
		}

		[Fact]
		public void ShouldResolveAttachmentConflictInFavorOfLocalVersion()
		{
			AttachmentConflictResolveTest(StraightforwardConflictResolution.ResolveToLocal);
		}

		[Fact]
		public void ShouldResolveAttachmentConflictInFavorOfRemoteVersion()
		{
			AttachmentConflictResolveTest(StraightforwardConflictResolution.ResolveToRemote);
		}

		private void DocumentConflictResolveTest(StraightforwardConflictResolution docConflictResolution)
		{
			var master = CreateStore();
			var slave = CreateStore();

			SetupReplication(master.DatabaseCommands, slave);

			using (var session = slave.OpenSession())
			{
				session.Store(new ReplicationConfig()
				{
					DocumentConflictResolution = docConflictResolution
				}, Constants.RavenReplicationConfig);

				session.Store(new Item()
				{
					Name = "local"
				}, "items/1");

				session.SaveChanges();
			}

			using (var session = master.OpenSession())
			{
				session.Store(new Item()
				{
					Name = "remote"
				}, "items/1");

				session.Store(new
				{
					Foo = "marker"
				}, "marker");

				session.SaveChanges();
			}

			WaitForReplication(slave, "marker");

			using (var session = slave.OpenSession())
			{
				Item item = null;

				Assert.DoesNotThrow(() => { item = session.Load<Item>("items/1"); });

				Assert.Equal(docConflictResolution == StraightforwardConflictResolution.ResolveToLocal ? "local" : "remote",
				             item.Name);
			}
		}

		private void AttachmentConflictResolveTest(StraightforwardConflictResolution attachmentConflictResolution)
		{
			var master = CreateStore();
			var slave = CreateStore();

			SetupReplication(master.DatabaseCommands, slave);

			using (var session = slave.OpenSession())
			{
				session.Store(new ReplicationConfig()
				{
					AttachmentConflictResolution = attachmentConflictResolution
				}, Constants.RavenReplicationConfig);

				session.SaveChanges();
			}

			var local = new byte[] {1, 2, 3, 4};
			var remote = new byte[] {3, 2, 1};

			slave.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(local), new RavenJObject());

			master.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(remote), new RavenJObject());

			master.DatabaseCommands.PutAttachment("marker", null, new MemoryStream(), new RavenJObject());

			WaitForAttachment(slave, "marker");

			Attachment attachment = null;
			Assert.DoesNotThrow(() => { attachment = slave.DatabaseCommands.GetAttachment("attach/1"); });

			switch (attachmentConflictResolution)
			{
				case StraightforwardConflictResolution.ResolveToLocal:
					Assert.Equal(local, attachment.Data().ReadData());
					break;
				case StraightforwardConflictResolution.ResolveToRemote:
					Assert.Equal(remote, attachment.Data().ReadData());
					break;
				default:
					throw new ArgumentOutOfRangeException("attachmentConflictResolution");
			}
		}
	}
}