using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Xunit;

namespace Raven.Tests.Storage
{
	public class IndexStaleViaEtags : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public IndexStaleViaEtags()
		{
			db =
				new DocumentDatabase(new RavenConfiguration
				{DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanTellThatIndexIsStale()
		{
			db.TransactionalStorage.Batch(accessor => Assert.False(accessor.Tasks.IsIndexStale("Raven/DocumentsByEntityName",null, null)));

			db.Put("ayende", null, new JObject(), new JObject(), null);

			db.TransactionalStorage.Batch(accessor => Assert.True(accessor.Tasks.IsIndexStale("Raven/DocumentsByEntityName", null, null)));
		}

		[Fact]
		public void CanIndexDocuments()
		{
			db.SpinBackgroundWorkers();
			
			db.TransactionalStorage.Batch(accessor => Assert.False(accessor.Tasks.IsIndexStale("Raven/DocumentsByEntityName", null, null)));

			db.Put("ayende", null, new JObject(), new JObject(), null);

			for (int i = 0; i < 50; i++)
			{
				bool indexed = false;
				db.TransactionalStorage.Batch(accessor => indexed = (accessor.Tasks.IsIndexStale("Raven/DocumentsByEntityName", null, null)));
				if (indexed == false)
					break;
				Thread.Sleep(50);
			}

			db.TransactionalStorage.Batch(accessor => Assert.False(accessor.Tasks.IsIndexStale("Raven/DocumentsByEntityName", null, null)));
		}
	}
}