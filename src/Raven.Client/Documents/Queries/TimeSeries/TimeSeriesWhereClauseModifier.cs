using System.Linq.Expressions;
using Raven.Client.Util;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesWhereClauseModifier : ExpressionVisitor
    {
        private readonly string _parameter;

        public TimeSeriesWhereClauseModifier(string parameter)
        {
            _parameter = parameter;
        }

        public Expression Modify(Expression expression)
        {
            return Visit(expression);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ParameterExpression p && p.Name == _parameter)
                return Expression.Parameter(node.Type, node.Member.Name);

            if (JavascriptConversionExtensions.IsWrappedConstantExpression(node))
            {
                // todo aviv
            }

            return base.VisitMember(node);
        }
    }
}
