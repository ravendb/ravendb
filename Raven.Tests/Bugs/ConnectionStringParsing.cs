using Raven.Abstractions.Data;
using Raven.Tests.Common;

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
	}
}