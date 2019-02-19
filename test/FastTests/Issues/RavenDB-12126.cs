using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12126 : NoDisposalNeeded
    {
        [Fact]
        public void CanParseQueryWithStringQuotesInComment()
        {
            var parser = new QueryParser();

            string script = @"
from AnalysisConfigurations as d
update {
    // this shouldn't be a problem
    d.Deleted = true;
    /*  ' */
}
";

            parser.Init(script);
            parser.Parse(QueryType.Update);
        }

        [Fact]
        public void CanParseQueryWithStringQuotesInComment_Select()
        {
            var parser = new QueryParser();

            string script = @"
from AnalysisConfigurations as d
select {
    // this shouldn't be a problem
    Deleted: true
    /*  ' */
}
";

            parser.Init(script);
            parser.Parse(QueryType.Select);
        }

    }
}
