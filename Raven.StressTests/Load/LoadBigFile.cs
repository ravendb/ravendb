using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Raven.Tests;
using Xunit;

namespace Raven.StressTests.Load
{
	public class LoadBigFile : RavenTest
	{
		[Fact]
		public async Task ShouldTakeUnder30Minutes()
		{
			var sw = Stopwatch.StartNew();

		    using (var store = NewDocumentStore())
			{
				using (var stream = typeof(LoadBigFile).Assembly.GetManifestResourceStream("Raven.StressTests.Load.LoadBigFile.dump"))
				{
					var dataDumper = new DataDumper(store.DocumentDatabase)
					{
						Progress = Console.WriteLine
					};
					await dataDumper.ImportData(new SmugglerOptions(), stream);
				}
			}
			sw.Stop();

			Assert.True(sw.Elapsed < TimeSpan.FromMinutes(30), string.Format("Test should run under 30 minutes, but run {0} minutes.", sw.Elapsed.TotalMinutes));
		}
	}
}