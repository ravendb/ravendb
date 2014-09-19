using Raven.Abstractions.Data;
using Raven.Tests.Common;
using System;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ConnectionStringParsing : NoDisposalNeeded
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
            var parser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("Url=http://localhost:8080;user=beam;password=up;ResourceManagerId=d5723e19-92ad-4531-adad-8611e6e05c8a");
            parser.Parse();
            var options = parser.ConnectionStringOptions;
            Assert.Equal("http://localhost:8080", options.Url);
            Assert.Equal(new Guid("d5723e19-92ad-4531-adad-8611e6e05c8a"), options.ResourceManagerId);
            Assert.NotNull(options.Credentials);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithEmbeddedOptionTypes_Successful()
        {
            var parser = ConnectionStringParser<EmbeddedRavenConnectionStringOptions>.FromConnectionString("Url=http://localhost:8080;user=beam;password=up;ResourceManagerId=d5723e19-92ad-4531-adad-8611e6e05c8a;memory=true");
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            Assert.Equal("http://localhost:8080", options.Url);
            Assert.Equal(new Guid("d5723e19-92ad-4531-adad-8611e6e05c8a"), options.ResourceManagerId);
            Assert.NotNull(options.Credentials);
            Assert.True(options.RunInMemory);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithFilesOptionTypes_Successful()
        {
            var parser = ConnectionStringParser<FilesConnectionStringOptions>.FromConnectionString("Url=http://localhost:8080;user=beam;password=up;filesystem=test");
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            Assert.Equal("http://localhost:8080", options.Url);
            Assert.NotNull(options.Credentials);
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