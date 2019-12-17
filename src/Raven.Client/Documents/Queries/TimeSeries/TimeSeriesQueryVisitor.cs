using System;
using System.Collections;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using Raven.Client.Documents.Linq;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesQueryVisitor<T>
    {
        private readonly MethodCallExpression _expression;
        private readonly RavenQueryProviderProcessor<T> _providerProcessor;
        private TimeSeriesWhereClauseModifier<T> _modifier;
        private StringBuilder _selectFields;
        private string _name, _between, _where, _groupBy, _loadTag;

        public TimeSeriesQueryVisitor(MethodCallExpression expression, RavenQueryProviderProcessor<T> processor)
        {
            _expression = expression;
            _providerProcessor = processor;
        }

        private void VisitMethod(MethodCallExpression mce)
        {
            switch (mce.Method.Name)
            {
                case "Where":
                    Where(mce.Arguments[0]);
                    break;
                case "GroupBy":
                    GroupBy(mce.Arguments[0]);
                    break;
                case "Select":
                    Select(mce.Arguments[0]);
                    break;
                case "LoadTag":
                    if (_loadTag == null)
                        throw new InvalidOperationException("Cannot understand how to translate " + _expression);
                    break;
                case "ToList":
                    break;
                default:
                    throw new InvalidOperationException("Cannot understand how to translate " + _expression);

            }
        }

        private void Where(Expression expression)
        {
            if (!(expression is UnaryExpression unary &&
                  unary.Operand is LambdaExpression lambda))
                throw new InvalidOperationException("Cannot understand how to translate " + _expression);

            _modifier = new TimeSeriesWhereClauseModifier<T>(lambda.Parameters[0].Name, _providerProcessor.DocumentQuery);

            if (lambda.Parameters.Count == 2) // Where((ts, tag) => ...)
                LoadTag(lambda.Parameters[1].Name);
            
            if (lambda.Body is BinaryExpression be)
                WhereBinary(be);
            else if (lambda.Body is MethodCallExpression call)
                WhereMethod(call);
            else
                throw new InvalidOperationException("Cannot understand how to translate " + _expression);
        }

        private void LoadTag(string alias)
        {
            _loadTag = $" load Tag as {alias}";
        }

        private void GroupBy(Expression expression)
        {
            string timePeriod;

            if (expression is ConstantExpression constantExpression)
            {
                timePeriod = constantExpression.Value.ToString();
            }

            else if (expression is LambdaExpression lambda)
            {
                if (!(lambda.Body is MethodCallExpression mce) ||
                    mce.Method.DeclaringType != typeof(ITimeSeriesGroupByBuilder))
                    throw new InvalidOperationException("Cannot understand how to translate " + _expression);

                var duration = ((ConstantExpression)mce.Arguments[0]).Value;
                timePeriod = $"{duration} {mce.Method.Name}";
            }

            else
            {
                timePeriod = expression.ToString();
            }

            _groupBy = $" group by '{timePeriod}'";
        }

        private void WhereMethod(MethodCallExpression call)
        {
            if (call.Method.DeclaringType != typeof(RavenQueryableExtensions))
                throw new NotSupportedException("Method not supported: " + call.Method.Name);
            
            switch (call.Method.Name)
            {
                case nameof(RavenQueryableExtensions.In):
                    WhereIn(call);
                    break;
                case nameof(RavenQueryableExtensions.ContainsAny):
                case nameof(RavenQueryableExtensions.ContainsAll):
                    // todo aviv : support this?
                    throw new NotSupportedException("Method not supported: " + call.Method.Name);
                default:
                    throw new NotSupportedException("Method not supported: " + call.Method.Name);
            }
            
        }

        private void Select(Expression expression)
        {
            if (!(expression is UnaryExpression unary &&
                  unary.Operand is LambdaExpression lambda))
                throw new InvalidOperationException("Cannot understand how to translate " + _expression);

            var body = lambda.Body;
            _selectFields = new StringBuilder();

            switch (body.NodeType)
            {
                case ExpressionType.New:
                    var newExp = (NewExpression)body;
                    foreach (var c in newExp.Arguments)
                    {
                        if (!(c is MethodCallExpression selectCall))
                            throw new InvalidOperationException("Cannot understand how to translate " + _expression);
                        AddSelectField(selectCall.Method.Name);
                    }
                    break;
                case ExpressionType.MemberInit:
                    var initExp = (MemberInitExpression)body;
                    foreach (var c in initExp.Bindings)
                    {
                        AddSelectField(c.Member.Name);
                    }
                    break;
                case ExpressionType.Call:
                    var call = (MethodCallExpression)body;
                    AddSelectField(call.Method.Name);
                    break;
                default:
                    throw new InvalidOperationException("Cannot understand how to translate " + _expression);
            }
        }

        private void TimeSeriesName(MethodCallExpression mce)
        {
            if (!(mce.Arguments[1] is ConstantExpression constantExpression))
                throw new InvalidOperationException("Cannot understand how to translate " + _expression);

            if (mce.Arguments.Count == 1)
            {
                _name = constantExpression.Value.ToString();
            }
            else
            {
                var srcAlias = LinqPathProvider.RemoveTransparentIdentifiersIfNeeded(mce.Arguments[0].ToString());

                if (_providerProcessor.FromAlias == null)
                    _providerProcessor.AddFromAlias(srcAlias);
                
                _name = $"{srcAlias}.{constantExpression.Value}";
            }
        }

        private void Between(MethodCallExpression mce)
        {
            var from = GetDateValue(mce.Arguments[2]);
            var to = GetDateValue(mce.Arguments[3]);

            var p1 = _providerProcessor.DocumentQuery.ProjectionParameter(from);
            var p2 = _providerProcessor.DocumentQuery.ProjectionParameter(to);

            _between = $" between {p1} and {p2}";
        }

        private void WhereBinary(BinaryExpression expression)
        {
            Debug.Assert(_modifier != null);

            var filterExpression = ModifyExpression(expression);

            _where = $" where {filterExpression}";
        }

        private string ModifyExpression(Expression expression)
        {
            if (expression is BinaryExpression be && 
                (be.NodeType == ExpressionType.OrElse || be.NodeType == ExpressionType.AndAlso))
            {
                var left = ModifyExpression(be.Left);
                var right = ModifyExpression(be.Right);
                var op = expression.NodeType == ExpressionType.OrElse ? "or" : "and";

                return $"{left} {op} {right}";
            }

            return _modifier.Modify(expression).ToString();
        }

        private void WhereIn(MethodCallExpression mce)
        {
            Debug.Assert(_modifier != null);

            var exp = _modifier.Modify(mce.Arguments[0]);

            string path = exp is ParameterExpression p 
                ? p.Name 
                : exp.ToString();

            var objects = (IEnumerable)_providerProcessor.LinqPathProvider.GetValueFromExpression(mce.Arguments[1], typeof(IEnumerable));

            var parameter = _providerProcessor.DocumentQuery.ProjectionParameter(objects);

            _where = $" where {path} in ({parameter})";
        }

        public string VisitExpression()
        {
            var callExpression = _expression;
            while (callExpression != null)
            {
                if (callExpression.Object != null)
                {
                    if (!(callExpression.Object is MethodCallExpression inner))
                        throw new InvalidOperationException("Cannot understand how to translate " + callExpression);

                    VisitMethod(callExpression);
                    callExpression = inner;
                    continue;
                }

                TimeSeriesName(callExpression);

                if (callExpression.Arguments.Count == 4)
                    Between(callExpression);

                break;
            }

            return BuildQuery();
        }

        private string BuildQuery()
        {
            var queryBuilder = new StringBuilder();

            queryBuilder.Append("from ").Append(_name);

            if (_between != null)
                queryBuilder.Append(_between);
            if (_loadTag != null)
                queryBuilder.Append(_loadTag);
            if (_where != null)
                queryBuilder.Append(_where);
            if (_groupBy != null)
                queryBuilder.Append(_groupBy);
            if (_selectFields != null)
                queryBuilder.Append(" select ").Append(_selectFields);

            return queryBuilder.ToString();
        }

        private void AddSelectField(string name)
        {
            switch (name)
            {
                case "Max":
                case "Min":
                case "Sum":
                case "Count":
                    if (_selectFields.Length > 0)
                        _selectFields.Append(", ");
                    _selectFields.Append($"{name.ToLower()}()");
                    break;
                case "Average":
                    if (_selectFields.Length > 0)
                        _selectFields.Append(", ");
                    _selectFields.Append("avg()");
                    break;
                default:
                    throw new InvalidOperationException("Unsupported aggregation method " + name);
            }
        }

        private static string GetDateValue(Expression exp)
        {
            if (exp is ConstantExpression constant)
                return constant.Value.ToString();

            LinqPathProvider.GetValueFromExpressionWithoutConversion(exp, out var value);

            if (value is string s)
                return s;

            if (!(value is DateTime d))
                throw new InvalidOperationException("failed "); //todo aviv

            return d.GetDefaultRavenFormat();
        }
    }

}
