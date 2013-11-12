using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class TroyMapReduceImport : RavenTest
	{
		[Fact]
		public async Task CanGetCorrectResult()
		{
			using (var store = NewDocumentStore())
			{
				var smugglerOptions = new SmugglerOptions();
				var dataDumper = new DataDumper(store.DocumentDatabase);
				using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendump"))
				{
                    await dataDumper.ImportData(smugglerOptions, stream);
				}

				using(var s = store.OpenSession())
				{
					var objects = s.Query<object>("LogEntry/CountByDate")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
					Assert.Equal(4, objects.Count);
				}
			}
		}

		[Fact]
		public async Task CanGetCorrectResult_esent()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				var smugglerOptions = new SmugglerOptions();
				var dataDumper = new DataDumper(store.DocumentDatabase);
				using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendump"))
				{
                    await dataDumper.ImportData(smugglerOptions, stream);
				}

				WaitForUserToContinueTheTest(store);
				using (var s = store.OpenSession())
				{
					var objects = s.Query<object>("LogEntry/CountByDate")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
					Assert.Equal(4, objects.Count);
				}
			}
		}
	}
}