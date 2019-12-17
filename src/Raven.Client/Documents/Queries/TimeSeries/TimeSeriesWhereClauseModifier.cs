using System.Linq.Expressions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Util;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesWhereClauseModifier<T> : ExpressionVisitor
    {
        private readonly string _parameter;

        private readonly IAbstractDocumentQuery<T> _documentQuery;

        private bool _arrayIndex;

        public TimeSeriesWhereClauseModifier(string parameter, IAbstractDocumentQuery<T> documentQuery)
        {
            _parameter = parameter;
            _documentQuery = documentQuery;
        }

        public Expression Modify(Expression expression)
        {
            // - removes the lambda parameter from filter expression (e.g. 'ts.Tag' => 'Tag')
            // - turns constant expressions into query parameters 

            return Visit(expression);
        }

        public override Expression Visit(Expression node)
        {
            if (node.NodeType != ExpressionType.ArrayIndex)
                return base.Visit(node);

            _arrayIndex = true;
            var expression = base.Visit(node);
            _arrayIndex = false;

            return expression;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ParameterExpression p && p.Name == _parameter)
                return Expression.Parameter(node.Type, node.Member.Name);

            if (JavascriptConversionExtensions.IsWrappedConstantExpression(node))
                return AddAsQueryParameter(node);
            
            return base.VisitMember(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (_arrayIndex == false) 
                return AddAsQueryParameter(node);

            return base.VisitConstant(node);
        }

        private ParameterExpression AddAsQueryParameter(Expression node)
        {
            LinqPathProvider.GetValueFromExpressionWithoutConversion(node, out var value);
            var queryParameter = _documentQuery.ProjectionParameter(value);

            return Expression.Parameter(node.Type, queryParameter);
        }
    }
}
