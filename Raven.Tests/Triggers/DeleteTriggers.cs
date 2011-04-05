//-----------------------------------------------------------------------
// <copyright file="DeleteTriggers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Triggers
{
	public class DeleteTriggers : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public DeleteTriggers()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				Container = new CompositionContainer(new TypeCatalog(
					typeof(CascadeDeleteTrigger))),
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanCascadeDeletes()
		{
			db.Put("abc", null, RavenJObject.Parse("{name: 'a'}"), RavenJObject.Parse("{'Cascade-Delete': 'def'}"), null);
			db.Put("def", null, RavenJObject.Parse("{name: 'b'}"), new RavenJObject(), null);

			db.Delete("abc", null, null);

			Assert.Null(db.Get("def", null));
		}
		
	}
}
