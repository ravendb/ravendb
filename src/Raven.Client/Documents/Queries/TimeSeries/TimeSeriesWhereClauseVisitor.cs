using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Util;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesWhereClauseVisitor<T> : ExpressionVisitor
    {
        private readonly string _alias;

        private readonly IAbstractDocumentQuery<T> _documentQuery;

        private bool _arrayIndex;

        internal List<string> Parameters;

        private (string From, string To) _renameTagAlias;

        public TimeSeriesWhereClauseVisitor(string alias, IAbstractDocumentQuery<T> documentQuery)
        {
            _alias = alias;
            _documentQuery = documentQuery;
        }

        public void Rename(string from, string to)
        {
            if (from == to)
                return;
            
            _renameTagAlias = (from, to);
        }

        public Expression VisitWhere(Expression expression)
        {
            // - removes the lambda parameter from filter expression (e.g. 'ts.Tag' => 'Tag')
            // - turns constant expressions into query parameters 
            // - adds parameter expressions to Parameters

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

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Parameters ??= new List<string>();

            if (ShouldRename(node))
            {
                var renamed = _renameTagAlias.To;
                Parameters.Add(renamed);
                return Expression.Parameter(node.Type, renamed);
            }
            
            Parameters.Add(node.Name);

            return base.VisitParameter(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is MemberExpression innerMember && 
                IsWhereAlias(innerMember.Expression) &&
                innerMember.Member.Name == "Value" && 
                innerMember.Type != typeof(double))
            {
                // e.g. 'Where(entry => entry.Value.NamedVal ... )'
                // do not include 'entry.Value' in generated RQL, just 'NamedVal'
                return Expression.Parameter(node.Type, node.Member.Name);
            }

            if (IsWhereAlias(node.Expression))
            {
                return Expression.Parameter(node.Type, node.Member.Name);
            }

            if (JavascriptConversionExtensions.IsWrappedConstantExpression(node))
                return AddAsQueryParameter(node);
            
            return base.VisitMember(node);
        }

        private bool IsWhereAlias(Expression expression)
        {
            return expression is ParameterExpression p && p.Name == _alias;
        }

        private bool ShouldRename(ParameterExpression expression)
        {
            return _renameTagAlias.From == expression.Name;
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
