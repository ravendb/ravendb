using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ConnectionStringParsing
	{
		[Fact]
		public void EnsureWellFormedConnectionStrings_ParsingWithEndingSemicolons_Successful()
		{
			new DocumentStore().ParseConnectionString("Url=http://localhost:10301");
			new DocumentStore().ParseConnectionString("Url=http://localhost:10301/");
			new DocumentStore().ParseConnectionString("Url=http://localhost:10301;");
			new DocumentStore().ParseConnectionString("Url=http://localhost:10301/;");
		}
	}
}