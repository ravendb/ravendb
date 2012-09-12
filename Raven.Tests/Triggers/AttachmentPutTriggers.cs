//-----------------------------------------------------------------------
// <copyright file="AttachmentPutTriggers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Triggers
{
	public class AttachmentPutTriggers: RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public AttachmentPutTriggers()
		{
			store = NewDocumentStore(catalog:(new TypeCatalog(typeof (AuditAttachmentPutTrigger), typeof(RefuseBigAttachmentPutTrigger))));
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}


		[Fact]
		public void CanModifyAttachmentPut()
		{
			db.PutStatic("ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

			var attachment = db.GetStatic("ayende");
			Assert.Equal(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), attachment.Metadata.Value<DateTime>("created_at"));
		}


		[Fact]
		public void CanVetoAttachmentPut()
		{
			var operationVetoedException = Assert.Throws<OperationVetoedException>(() =>
																					   db.PutStatic("ayende", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 }),
																									new RavenJObject()));

			Assert.Equal("PUT vetoed by Raven.Tests.Triggers.RefuseBigAttachmentPutTrigger because: Attachment is too big", operationVetoedException.Message);
		}
	}
}