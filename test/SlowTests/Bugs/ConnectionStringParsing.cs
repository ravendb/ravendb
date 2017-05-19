using System;
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
        public void EnsureWellFormedConnectionStrings_Parsing_FailWithUnknownParameter()
        {
            var dbParser = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString("memory=true");
            Assert.Throws<ArgumentException>(() => dbParser.Parse());
        }
    }
}
