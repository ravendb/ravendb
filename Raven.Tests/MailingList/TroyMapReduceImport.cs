using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class TroyMapReduceImport : RavenTest
	{
		[Fact]
		public void CanGetCorrectResult()
		{
			using (var store = NewDocumentStore())
			{
				var smugglerOptions = new SmugglerOptions();
				var dataDumper = new DataDumper(store.DocumentDatabase, smugglerOptions);
				using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendump"))
				{
					dataDumper.ImportData(stream, smugglerOptions);
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
		public void CanGetCorrectResult_esent()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				var smugglerOptions = new SmugglerOptions();
				var dataDumper = new DataDumper(store.DocumentDatabase, smugglerOptions);
				using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.Sandbox.ravendump"))
				{
					dataDumper.ImportData(stream, smugglerOptions);
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