using System.ComponentModel.Composition.Hosting;
using Newtonsoft.Json.Linq;
using Raven.Database;
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
			db.Put("abc", null, JObject.Parse("{name: 'a'}"), JObject.Parse("{'Cascade-Delete': 'def'}"), null);
			db.Put("def", null, JObject.Parse("{name: 'b'}"), new JObject(), null);

			db.Delete("abc", null, null);

			Assert.Null(db.Get("def", null));
		}
		
	}
}