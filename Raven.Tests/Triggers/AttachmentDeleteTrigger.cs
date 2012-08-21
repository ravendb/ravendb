//-----------------------------------------------------------------------
// <copyright file="AttachmentDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using System.IO;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Database.Plugins;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Triggers
{
	public class AttachmentDeleteTrigger: RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public AttachmentDeleteTrigger()
		{
			store = NewDocumentStore(new CompositionContainer(new TypeCatalog(typeof (RefuseAttachmentDeleteTrigger))));
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanVetoDeletes()
		{
			db.PutStatic("ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
			var operationVetoedException = Assert.Throws<OperationVetoedException>(()=>db.DeleteStatic("ayende", null));
			Assert.Equal("DELETE vetoed by Raven.Tests.Triggers.AttachmentDeleteTrigger+RefuseAttachmentDeleteTrigger because: Can't delete attachments", operationVetoedException.Message);
		}

		public class RefuseAttachmentDeleteTrigger: AbstractAttachmentDeleteTrigger
		{
			public override VetoResult AllowDelete(string key)
			{
				return VetoResult.Deny("Can't delete attachments");
			}
		}
	}
}