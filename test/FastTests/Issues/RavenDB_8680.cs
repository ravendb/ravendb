using Raven.Server.Documents.Queries.Parser;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_8680 : NoDisposalNeeded
    {
        [Theory]
        [InlineData("from Categories where id() = 'categories/8'", @"FROM Categories WHERE id() = 'categories/8'
")]
        [InlineData("from Categories where id() in ('categories/8')", @"FROM Categories WHERE id() IN ('categories/8')
")]
        public void CanPrintParsedQuery(string queryText, string expected)
        {
            var parser = new QueryParser();

            parser.Init(queryText);

            var query = parser.Parse();

            Assert.Equal(expected, query.ToString());
        }
    }
}
