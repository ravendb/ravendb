using System;
using System.Linq;
using FastTests;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Bugs
{
    public class ConnectionStringParsing : RavenTestBase
    {
        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithEndingSemicolons_Successful()
        {
            var connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("url=http://localhost:10301;");
            connectionStringParser.Parse();

            Assert.DoesNotContain(";", connectionStringParser.ConnectionStringOptions.Urls.First());

            connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("url=http://localhost:10301/;");
            connectionStringParser.Parse();

            Assert.DoesNotContain(";", connectionStringParser.ConnectionStringOptions.Urls.First());
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingWithRavenOptionTypes_Successful()
        {
            var parser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("url=http://localhost:8080;database=up;");
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            Assert.Equal("http://localhost:8080", options.Urls.First());
            Assert.Equal("up", options.Database);
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_Parsing_FailWithUnknownParameter()
        {
            var dbParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("memory=true");
            Assert.Throws<ArgumentException>(() => dbParser.Parse());
        }

        [Fact]
        public void EnsureWellFormedConnectionStrings_ParsingMultiUrls_Successful()
        {
            var connectionStringParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("url=http://localhost:10301; url=http://localhost:10302; url=http://localhost:10303; database=up;");
            connectionStringParser.Parse();

            var options = connectionStringParser.ConnectionStringOptions;
            Assert.True(options.Urls.Count == 3);
            Assert.Equal("up", options.Database);
        }
    }
}
