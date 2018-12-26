using System;
using System.Collections.Generic;
using Raven.Server.Documents.Queries.Parser;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_8680 : NoDisposalNeeded
    {
        public static IEnumerable<object[]> GetTestData()
        {
            yield return new object[] {"from Categories where id() = 'categories/8'", $"FROM Categories WHERE id() = 'categories/8'{Environment.NewLine}"};
            yield return new object[] {"from Categories where id() in ('categories/8')", $"FROM Categories WHERE id() IN ('categories/8'){Environment.NewLine}"};
        }

        [Theory]
        [MemberData(nameof(GetTestData))]
        public void CanPrintParsedQuery(string queryText, string expected)
        {
            var parser = new QueryParser();

            parser.Init(queryText);

            var query = parser.Parse();

            Assert.Equal(expected, query.ToString());
        }
    }
}
