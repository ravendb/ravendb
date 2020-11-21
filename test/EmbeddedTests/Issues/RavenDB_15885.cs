using System.IO;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests.Issues
{
    public class RavenDB_15885 : EmbeddedTestBase
    {
        [Fact]
        public void Can_Use_Custom_Analyzer()
        {
            var paths = CopyServer();
            CopyCustomAnalyzer(paths.ServerDirectory);

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                });

                using (var store = embedded.GetDocumentStore("DatabaseWithCustomAnalyzers"))
                {
                    new Index_With_Custom_Analyzer().Execute(store);
                }
            }
        }

        private void CopyCustomAnalyzer(string serverDirectory)
        {
            using (var source = typeof(RavenDB_15885).Assembly.GetManifestResourceStream("EmbeddedTests.Data.MyCustomAnalyzers.dll"))
            using (var destination = File.Create(Path.Combine(serverDirectory, "MyCustomAnalyzers.dll")))
            {
                source.CopyTo(destination);
            }
        }

        private class Index_With_Custom_Analyzer : AbstractIndexCreationTask<Company>
        {
            public Index_With_Custom_Analyzer()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name
                                   };

                Analyze(x => x.Name, "MyCustomAnalyzers.MyThrowingAnalyzer, MyCustomAnalyzers");
            }
        }

        private class Company
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
