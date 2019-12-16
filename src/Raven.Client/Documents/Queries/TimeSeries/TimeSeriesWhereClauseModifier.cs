using System.Linq.Expressions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Util;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesWhereClauseModifier<T> : ExpressionVisitor
    {
        private readonly string _parameter;

        private IAbstractDocumentQuery<T> _documentQuery;

        public TimeSeriesWhereClauseModifier(string parameter, IAbstractDocumentQuery<T> documentQuery)
        {
            _parameter = parameter;
            _documentQuery = documentQuery;
        }

        public Expression Modify(Expression expression)
        {
            return Visit(expression);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // - removes the lambda parameter from filter expression
            // - turns constant expressions into query parameters 

            if (node.Expression is ParameterExpression p && p.Name == _parameter)
                return Expression.Parameter(node.Type, node.Member.Name);

            if (JavascriptConversionExtensions.IsWrappedConstantExpression(node))
            {
                LinqPathProvider.GetValueFromExpressionWithoutConversion(node, out var value);
                var queryParameter = _documentQuery.ProjectionParameter(value);
                return Expression.Parameter(node.Type, queryParameter);
            }

            return base.VisitMember(node);
        }
    }
}
