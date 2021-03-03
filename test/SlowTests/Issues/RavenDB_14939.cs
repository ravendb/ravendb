using System.Collections.Generic;
using System.IO;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.Analysis;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14939 : RavenTestBase
    {
        public RavenDB_14939(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseCustomAnalyzer()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                {
                    { "MyAnalyzer", new AnalyzerDefinition
                    {
                        Name = "MyAnalyzer",
                        Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs")
                    }}
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CanUseCustomAnalyzerWithOperations()
        {
        }

        private static string GetAnalyzer(string name)
        {
            using (var stream = GetDump(name))
            using (var reader = new StreamReader(stream))
                return reader.ReadToEnd();
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_14939).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
