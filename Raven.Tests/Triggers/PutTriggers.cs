//-----------------------------------------------------------------------
// <copyright file="PutTriggers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using Raven.Client.Embedded;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Abstractions.Exceptions;
using Raven.Tests.Common;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Triggers
{
	public class PutTriggers : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public PutTriggers()
		{
			store = NewDocumentStore(catalog:(new TypeCatalog(typeof (VetoCapitalNamesPutTrigger), typeof(AuditPutTrigger))));
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanPutDocumentWithLowerCaseName()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);

			Assert.Contains("\"name\":\"abc\"", db.Documents.Get("abc", null).ToJson().ToString(Formatting.None));
		}

		[Fact]
		public void TriggerCanModifyDocumentBeforeInsert()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);

			var actualString = db.Documents.Get("abc", null).DataAsJson.ToString(Formatting.None);
			Assert.Contains("2010-02-13T18:26:48.5060000Z", actualString);
		}

		[Fact]
		public void CannotPutDocumentWithUpperCaseNames()
		{
			Assert.Throws<OperationVetoedException>(() => db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'ABC'}"), new RavenJObject(), null));
		}
	}
}