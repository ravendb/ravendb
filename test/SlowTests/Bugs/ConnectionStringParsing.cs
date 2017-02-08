using System;
using FastTests;
using Raven.NewClient.Abstractions.Data;
using Xunit;

namespace SlowTests.Bugs
{
    public class ConnectionStringParsing : RavenNewTestBase
    {
        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithEndingSemicolons_Successful()
        {
            var connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("Url=http://localhost:10301;");
            connectionStringParser.Parse();

            Assert.DoesNotContain(";", connectionStringParser.ConnectionStringOptions.Url);

            connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("Url=http://localhost:10301/;");
            connectionStringParser.Parse();

            Assert.DoesNotContain(";", connectionStringParser.ConnectionStringOptions.Url);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithRavenOptionTypes_Successful()
        {
            var parser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("Url=http://localhost:8080;user=beam;password=up;");
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            Assert.Equal("http://localhost:8080", options.Url);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithEmbeddedOptionTypes_Successful()
        {
            var parser = ConnectionStringParser<EmbeddedRavenConnectionStringOptions>.FromConnectionString("Url=http://localhost:8080;user=beam;password=up;memory=true");
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            Assert.Equal("http://localhost:8080", options.Url);
            Assert.True(options.RunInMemory);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithFilesOptionTypes_Successful()
        {
            var parser = ConnectionStringParser<FilesConnectionStringOptions>.FromConnectionString("Url=http://localhost:8080;user=beam;password=up;filesystem=test");
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            Assert.Equal("http://localhost:8080", options.Url);
            Assert.Equal("test", options.DefaultFileSystem);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_Parsing_FailWithUnknownParameter()
        {
            var filesParser = ConnectionStringParser<FilesConnectionStringOptions>.FromConnectionString("ResourceManagerId=d5723e19-92ad-4531-adad-8611e6e05c8a;");
            Assert.Throws<ArgumentException>(() => filesParser.Parse());

            var dbParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("memory=true");
            Assert.Throws<ArgumentException>(() => dbParser.Parse());

            var embeddedParser = ConnectionStringParser<EmbeddedRavenConnectionStringOptions>.FromConnectionString("filesystem=test;");
            Assert.Throws<ArgumentException>(() => embeddedParser.Parse());
        }
    }
}
