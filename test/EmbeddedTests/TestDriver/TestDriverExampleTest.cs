using System;
using System.IO;
using System.Threading.Tasks;
using EmbeddedTests.Platform;
using Raven.Client.Documents;
using Raven.TestDriver;
using Xunit;

namespace EmbeddedTests.TestDriver
{
    public class TestDriverExampleTest : RavenTestDriver
    {
        static TestDriverExampleTest()
        {
            ConfigureServer(new TestServerOptions
            {
                ServerDirectory = GetServerPath()
            });
        }

        private static string GetServerPath()
        {
            var prefix = PosixHelper.RunningOnPosix ? "file://" : "file:///";

            var testAssemblyLocation = typeof(RavenTestDriver).Assembly.CodeBase;
            if (testAssemblyLocation.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                testAssemblyLocation = testAssemblyLocation.Substring(prefix.Length);

            var testDllFile = new FileInfo(testAssemblyLocation);
            if (testDllFile.Exists == false)
                throw new InvalidOperationException($"Could not find '{testDllFile.FullName}'.");

#if DEBUG
            var serverDirectory = @"../../../../../src/Raven.Server/bin/x64/Debug/netcoreapp3.1";
            if (Directory.Exists(serverDirectory) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                serverDirectory = @"../../../../../src/Raven.Server/bin/Debug/netcoreapp3.1";
#else
            var serverDirectory = @"../../../../../src/Raven.Server/bin/x64/Release/netcoreapp3.1";
            if (Directory.Exists(serverDirectory) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                serverDirectory = @"../../../../../src/Raven.Server/bin/Release/netcoreapp3.1";
#endif

            var path = Path.Combine(testDllFile.DirectoryName, serverDirectory);
            if (PosixHelper.RunningOnPosix)
                path = PosixHelper.FixLinuxPath(path);

            return Path.GetFullPath(path);
        }

        private const string ExampleDocId = "TestDriver/Item";

        private const string DocFromDumpId = "Test/1";

        private class TestDoc
        {
            public string Name { get; set; }
        }

        private class TestDocConvention
        {
            public string Name { get; set; }
            public string Email { get; set; }
        }

        private readonly TestDocConvention _testDocConvention = new TestDocConvention
        {
            Name = "Test",
            Email = "test@local"
        };

        private readonly TestDoc _doc = new TestDoc
        {
            Name = "Test"
        };

        protected override Stream DatabaseDumpFileStream =>
            typeof(TestDriverExampleTest)
            .Assembly
            .GetManifestResourceStream("EmbeddedTests.Data.testing.ravendbdump");

        protected override void PreInitialize(IDocumentStore documentStore)
        {
            documentStore.Conventions.RegisterAsyncIdConvention<TestDocConvention>((dbname, testDoc) => Task.FromResult(testDoc.Email));
        }

        protected override void SetupDatabase(IDocumentStore documentStore)
        {
            using (var session = documentStore.OpenSession())
            {
                session.Store(_doc, ExampleDocId);
                session.Store(_testDocConvention);
                session.SaveChanges();
            }
        }

        [Fact]
        public void ShouldLoadDocumentAddedInSetupDatabasePhase()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var item = session.Load<TestDoc>(ExampleDocId);
                    Assert.Equal(_doc.Name, item.Name);
                    item.Name = "Example";
                    session.SaveChanges();

                    var item2 = session.Load<TestDoc>(ExampleDocId);
                    Assert.Equal(item.Name, item2.Name);
                }
            }
        }

        [Fact]
        public void ShouldLoadDocumentAddedViaDatabaseImport()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var docFromImport = session.Load<TestDoc>(DocFromDumpId);
                    Assert.NotNull(docFromImport);
                    Assert.Equal("This is a test", docFromImport.Name);
                }
            }
        }

        [Fact]
        public void ShouldLoadDocumentThatUsesIdConvention()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var docWithConvention = session.Load<TestDocConvention>("test@local");
                    Assert.NotNull(docWithConvention);
                    Assert.Equal("Test", docWithConvention.Name);
                }
            }
        }
    }
}
