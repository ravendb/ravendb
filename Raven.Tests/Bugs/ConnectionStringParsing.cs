using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ConnectionStringParsing
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
	}
}