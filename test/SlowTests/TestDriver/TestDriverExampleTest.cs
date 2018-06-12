using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.TestDriver;
using Xunit;

namespace SlowTests.TestDriver
{
    public class RavenServerInDebugDllLocator : RavenServerLocator
    {
        public override string ServerPath
        {
            get
            {
                var testAssemblyLocation = typeof(RavenTestDriver<>).Assembly.Location;
                var testDllFile = new FileInfo(testAssemblyLocation);

#if DEBUG
                var serverDllPath = @"../../../../../src/Raven.Server/bin/x64/Debug/netcoreapp2.1/Raven.Server.dll";
                if (File.Exists(serverDllPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                    serverDllPath = @"../../../../../src/Raven.Server/bin/Debug/netcoreapp2.1/Raven.Server.dll";
#else
                var serverDllPath = @"../../../../../src/Raven.Server/bin/x64/Release/netcoreapp2.1/Raven.Server.dll";
                if (File.Exists(serverDllPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                    serverDllPath = @"../../../../../src/Raven.Server/bin/Release/netcoreapp2.1/Raven.Server.dll";
#endif

                var serverPath = Path.Combine(
                    testDllFile.DirectoryName,
                    serverDllPath);
                return serverPath;
            }
        }

        public override string Command => "dotnet";

        public override string CommandArguments => ServerPath;
    }

    public class TestDriverExampleTest : RavenTestDriver<RavenServerInDebugDllLocator>
    {
        private const string ExampleDocId = "TestDriver/Item";

        private const string DocFromDumpId = "Test/1";

        //public TestDriverExampleTest()
        //{
        //    Debug = true;
        //}

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
            typeof(RavenServerInDebugDllLocator)
            .Assembly
            .GetManifestResourceStream("SlowTests.Data.testing.ravendbdump");

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
