//-----------------------------------------------------------------------
// <copyright file="PutTriggers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Triggers
{
	public class PutTriggers : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public PutTriggers()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
				Container = new CompositionContainer(new TypeCatalog(
					typeof(VetoCapitalNamesPutTrigger),
					typeof(AuditPutTrigger))),
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanPutDocumentWithLowerCaseName()
		{
			db.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);

			Assert.Contains("\"name\":\"abc\"", db.Get("abc", null).ToJson().ToString(Formatting.None));
		}

		[Fact]
		public void TriggerCanModifyDocumentBeforeInsert()
		{
			db.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);

			var actualString = db.Get("abc", null).DataAsJson.ToString(Formatting.None);
			Assert.Contains("2010-02-13T18:26:48.506Z", actualString);
		}

		[Fact]
		public void CannotPutDocumentWithUpperCaseNames()
		{
			Assert.Throws<OperationVetoedException>(() => db.Put("abc", null, RavenJObject.Parse("{'name': 'ABC'}"), new RavenJObject(), null));
		}
	}
}