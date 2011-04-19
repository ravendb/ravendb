using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ConnectionStringParsing
	{
		[Fact]
		public void EnsureWellFormedConnectionStrings_ParsingWithEndingSemicolons_Successful()
		{
			var documentStore = new DocumentStore();
			documentStore.ParseConnectionString("Url=http://localhost:10301;");
			Assert.DoesNotContain(";", documentStore.Url);
			documentStore.ParseConnectionString("Url=http://localhost:10301/;");
			Assert.DoesNotContain(";", documentStore.Url);
		}
	}
}