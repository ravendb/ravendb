using System.Text;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session.Tokens
{
    public class QueryOperatorToken : QueryToken
    {
        private readonly QueryOperator _queryOperator;

        private QueryOperatorToken(QueryOperator queryOperator)
        {
            _queryOperator = queryOperator;
        }

        public static QueryOperatorToken And = new QueryOperatorToken(QueryOperator.And);

        public static QueryOperatorToken Or = new QueryOperatorToken(QueryOperator.Or);

        public override void WriteTo(StringBuilder writer)
        {
            if (_queryOperator == QueryOperator.And)
            {
                writer.Append("AND");
                return;
            }

            writer.Append("OR");
        }
    }
}