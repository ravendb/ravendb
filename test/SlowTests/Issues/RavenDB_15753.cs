using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15753 : RavenTestBase
    {
        public RavenDB_15753(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void AdditionalAssemblies_Runtime()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("System.Xml"),
                        AdditionalAssembly.FromRuntime("System.Xml.ReaderWriter"),
                        AdditionalAssembly.FromRuntime("System.Private.Xml")
                    }
                }));
            }
        }

        [Fact]
        public void AdditionalAssemblies_Runtime_InvalidName()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("Some.Assembly.That.Does.Not.Exist")
                    }
                })));

                Assert.Contains("Cannot load assembly 'Some.Assembly.That.Does.Not.Exist'", e.Message);
                Assert.Contains("Could not load file or assembly 'Some.Assembly.That.Does.Not.Exist", e.Message);
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("System.Private.Xml"),
                        AdditionalAssembly.FromNuGet("System.Xml.ReaderWriter", "4.3.1")
                    }
                }));
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet_InvalidName()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("Some.Assembly.That.Does.Not.Exist", "4.3.1")
                    }
                })));

                Assert.Contains("Cannot load NuGet package 'Some.Assembly.That.Does.Not.Exist'", e.Message);
                Assert.Contains("Package does not exist", e.Message);
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet_InvalidSource()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("System.Xml.ReaderWriter", "4.3.1", "http://some.url.that.does.not.exist.com")
                    }
                })));

                Assert.Contains("Cannot load NuGet package 'System.Xml.ReaderWriter' version '4.3.1' from 'http://some.url.that.does.not.exist.com'", e.Message);
                Assert.Contains("Unable to load the service index for source", e.Message);
            }
        }
    }
}
