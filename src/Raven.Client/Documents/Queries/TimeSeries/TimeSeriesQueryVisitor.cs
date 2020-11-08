using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesQueryVisitor<T>
    {
        private readonly RavenQueryProviderProcessor<T> _providerProcessor;
        private TimeSeriesWhereClauseVisitor<T> _whereVisitor;
        private StringBuilder _selectFields;
        private string _src, _between, _where, _groupBy, _last, _first, _loadTag, _offset;

        public List<string> Parameters { get; internal set; }

        public TimeSeriesQueryVisitor(RavenQueryProviderProcessor<T> processor)
        {
            _providerProcessor = processor;
        }

        private void VisitMethod(MethodCallExpression mce)
        {
            switch (mce.Method.Name)
            {
                case nameof(ITimeSeriesQueryable.Where):
                    Where(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesQueryable.GroupBy):
                    GroupBy(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesAggregationQueryable.Select):
                    Select(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesAggregationQueryable.Offset):
                    Offset(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesQueryable.FromLast):
                    Last(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesQueryable.FromFirst):
                    First(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesQueryable.LoadByTag):
                case nameof(ITimeSeriesQueryable.ToList):
                    break;
                default:
                    throw new NotSupportedException("Method not supported: " + mce.Method.Name);

            }
        }

        private void Where(Expression expression)
        {
            if (_where != null)
                throw new InvalidOperationException("Cannot have multiple Where calls in TimeSeries functions ");

            if (!(expression is UnaryExpression unary &&
                  unary.Operand is LambdaExpression lambda))
                throw new NotSupportedException("Unsupported expression in Where clause " + expression);

            _whereVisitor = new TimeSeriesWhereClauseVisitor<T>(lambda.Parameters[0].Name, _providerProcessor.DocumentQuery);

            if (lambda.Parameters.Count == 2) // Where((ts, tag) => ...)
                LoadByTag(lambda.Parameters[1].Name);
            
            if (lambda.Body is BinaryExpression be)
                WhereBinary(be);
            else if (lambda.Body is MethodCallExpression call)
                WhereMethod(call);
            else
                throw new NotSupportedException("Unsupported expression in Where clause " + expression);
        }

        private void LoadByTag(string alias)
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

            else if (JavascriptConversionExtensions.IsWrappedConstantExpression(expression))
            {
                LinqPathProvider.GetValueFromExpressionWithoutConversion(expression, out var value);
                timePeriod = value.ToString();
            }

            else if (expression is LambdaExpression lambda)
            {
                timePeriod = GetTimePeriod(lambda, nameof(ITimeSeriesQueryable.GroupBy));
            }

            else
            {
                timePeriod = expression.ToString();
            }

            _groupBy = $" group by '{timePeriod}'";
        }

        private void WhereMethod(MethodCallExpression call)
        {
            switch (call.Method.Name)
            {
                case nameof(RavenQueryableExtensions.In):
                    WhereIn(call);
                    break;
                // todo RavenDB-14382
                case nameof(RavenQueryableExtensions.ContainsAny):
                case nameof(RavenQueryableExtensions.ContainsAll):
                case nameof(Enumerable.Any):
                default:
                    throw new NotSupportedException("Unsupported method in Where clause: " + call.Method.Name);
            }

        }

        private void Select(Expression expression)
        {
            if (_selectFields != null)
                throw new InvalidOperationException("Cannot have multiple Select calls in TimeSeries functions ");

            if (!(expression is UnaryExpression unary &&
                  unary.Operand is LambdaExpression lambda))
                throw new InvalidOperationException("Invalid Select expression " + expression);

            var body = lambda.Body;
            _selectFields = new StringBuilder();

            switch (body.NodeType)
            {
                case ExpressionType.New:
                    var newExp = (NewExpression)body;
                    foreach (var c in newExp.Arguments)
                    {
                        if (!(c is MethodCallExpression selectCall))
                            throw new InvalidOperationException("Invalid Select argument " + c);
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
                    throw new InvalidOperationException("Invalid Select expression " + expression);
            }
        }

        private void Offset(Expression expression)
        {
            if (expression.Type != typeof(TimeSpan))
                throw new InvalidOperationException($"Invalid argument in method '{nameof(ITimeSeriesAggregationQueryable.Offset)}'. " +
                                                    $"Expected argument of type 'TimeSpan' but got : '{expression.Type}'");

            LinqPathProvider.GetValueFromExpressionWithoutConversion(expression, out var ts);

            _offset = $" offset '{ts}'";
        }

        private void TimeSeriesCall(MethodCallExpression mce)
        {
            if (mce.Arguments.Count == 1)
            {
                _src = GetNameFromArgument(mce.Arguments[0]);
            }
            else
            {
                var sourceAlias = LinqPathProvider.RemoveTransparentIdentifiersIfNeeded(mce.Arguments[0].ToString());
                Parameters = new List<string>();

                if (_providerProcessor.FromAlias == null)
                {
                    _providerProcessor.AddFromAlias(sourceAlias);
                    Parameters.Add(sourceAlias);
                }
                else
                {
                    if (mce.Arguments[0] is ParameterExpression)
                    {
                        Parameters.Add(sourceAlias);
                    }
                    else
                    {
                        Parameters.Add(_providerProcessor.FromAlias);
                        if (sourceAlias != _providerProcessor.FromAlias)
                        {
                            Parameters.Add(sourceAlias);
                        }
                    }
                }

                _src = GetNameFromArgument(mce.Arguments[1]);

                if (mce.Arguments[1] is ParameterExpression == false)
                    _src = $"{sourceAlias}.{_src}";

                if (mce.Arguments.Count == 4)
                    Between(mce);
            }

            if (_whereVisitor?.Parameters == null) 
                return;

            Parameters ??= new List<string>();
            Parameters.AddRange(_whereVisitor.Parameters);

        }

        private void Last(Expression expression)
        {
            if (_first != null)
                throw new InvalidQueryException($"Cannot use both '{nameof(ITimeSeriesQueryable.FromFirst)}' and '{nameof(ITimeSeriesQueryable.FromLast)}' in the same Time Series query function ");

            if (!(expression is LambdaExpression lambda))
            {
                ThrowInvalidMethodArgument(expression, nameof(ITimeSeriesQueryable.FromLast));
                return;
            }

            var timePeriod = GetTimePeriod(lambda, nameof(ITimeSeriesQueryable.FromLast));

            _last = $" last {timePeriod}";
        }

        private void First(Expression expression)
        {
            if (_last != null)
                throw new InvalidQueryException($"Cannot use both '{nameof(ITimeSeriesQueryable.FromFirst)}' and '{nameof(ITimeSeriesQueryable.FromLast)}' in the same Time Series query function ");

            if (!(expression is LambdaExpression lambda))
            {
                ThrowInvalidMethodArgument(expression, nameof(ITimeSeriesQueryable.FromFirst));
                return;
            }

            var timePeriod = GetTimePeriod(lambda, nameof(ITimeSeriesQueryable.FromFirst));

            _first = $" first {timePeriod}";
        }

        private string GetNameFromArgument(Expression argument)
        {
            if (argument is ConstantExpression constantExpression)
                 return constantExpression.Value.ToString();

            if (JavascriptConversionExtensions.IsWrappedConstantExpression(argument))
            {
                LinqPathProvider.GetValueFromExpressionWithoutConversion(argument, out var value);
                return value.ToString();
            }

            if (argument is ParameterExpression p)
            {
                Parameters ??= new List<string>();
                Parameters.Add(p.Name);
                return p.Name;
            }

            throw new InvalidOperationException("Invalid TimeSeries argument " + argument);
        }

        private void Between(MethodCallExpression mce)
        {
            if (_first != null)
                throw new InvalidQueryException($"Cannot use '{nameof(ITimeSeriesQueryable.FromFirst)}' when From/To dates are provided to the Time Series query function ");
            
            if (_last != null)
                throw new InvalidQueryException($"Cannot use '{nameof(ITimeSeriesQueryable.FromLast)}' when From/To dates are provided to the Time Series query function ");

            var from = GetDateValue(mce.Arguments[2]);
            var to = GetDateValue(mce.Arguments[3]);

            if (!(mce.Arguments[2] is ParameterExpression))
                from = _providerProcessor.DocumentQuery.ProjectionParameter(from);

            if (!(mce.Arguments[3] is ParameterExpression))
                to = _providerProcessor.DocumentQuery.ProjectionParameter(to);

            _between = $" between {from} and {to}";
        }

        private void WhereBinary(BinaryExpression expression)
        {
            Debug.Assert(_whereVisitor != null);

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

            return _whereVisitor.VisitWhere(expression).ToString();
        }

        private void WhereIn(MethodCallExpression mce)
        {
            Debug.Assert(_whereVisitor != null);

            var exp = _whereVisitor.VisitWhere(mce.Arguments[0]);

            string path = exp is ParameterExpression p 
                ? p.Name 
                : exp.ToString();

            var objects = (IEnumerable)_providerProcessor.LinqPathProvider.GetValueFromExpression(mce.Arguments[1], typeof(IEnumerable));

            var parameter = _providerProcessor.DocumentQuery.ProjectionParameter(objects);

            _where = $" where {path} in ({parameter})";
        }

        public string Visit(MethodCallExpression expression)
        {
            try
            {
                VisitExpression(expression);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to translate expression to Time Series function : " + expression, e);
            }

            return BuildQuery();
        }

        private void VisitExpression(MethodCallExpression callExpression)
        {
            while (callExpression != null)
            {
                if (callExpression.Object != null)
                {
                    if (!(callExpression.Object is MethodCallExpression inner))
                        throw new InvalidOperationException("Unrecognized call expression " + callExpression);

                    VisitMethod(callExpression);
                    callExpression = inner;
                    continue;
                }

                TimeSeriesCall(callExpression);

                break;
            }
        }

        private string BuildQuery()
        {
            var queryBuilder = new StringBuilder();

            queryBuilder.Append("from ").Append(_src);

            if (_between != null)
                queryBuilder.Append(_between);
            if (_first != null)
                queryBuilder.Append(_first);
            if (_last != null)
                queryBuilder.Append(_last);
            if (_loadTag != null)
                queryBuilder.Append(_loadTag);
            if (_where != null)
                queryBuilder.Append(_where);
            if (_groupBy != null)
                queryBuilder.Append(_groupBy);
            if (_selectFields != null)
                queryBuilder.Append(" select ").Append(_selectFields);
            if (_offset != null)
                queryBuilder.Append(_offset);

            return queryBuilder.ToString();
        }

        private void AddSelectField(string name)
        {
            switch (name)
            {
                case nameof(ITimeSeriesGrouping.Max):
                case nameof(ITimeSeriesGrouping.Min):
                case nameof(ITimeSeriesGrouping.Sum):
                case nameof(ITimeSeriesGrouping.Count):
                case nameof(ITimeSeriesGrouping.First):
                case nameof(ITimeSeriesGrouping.Last):
                case nameof(ITimeSeriesGrouping.Average):
                    if (_selectFields.Length > 0)
                        _selectFields.Append(", ");
                    _selectFields.Append($"{name.ToLower()}()");
                    break;
                default:
                    throw new NotSupportedException("Unsupported aggregation method " + name);
            }
        }

        private string GetDateValue(Expression exp)
        {
            if (exp is ConstantExpression constant)
                return constant.Value.ToString();

            if (exp is ParameterExpression p)
            {
                Parameters ??= new List<string>();
                Parameters.Add(p.Name);
                return p.Name;
            }

            LinqPathProvider.GetValueFromExpressionWithoutConversion(exp, out var value);

            if (value is string s)
                return s;

            if (!(value is DateTime d))
                throw new InvalidOperationException("Invalid from/to arguments" + exp);

            return d.EnsureUtc().GetDefaultRavenFormat();
        }

        private static string GetTimePeriod(LambdaExpression lambda, string method)
        {
            if (!(lambda.Body is MethodCallExpression mce) ||
                mce.Method.DeclaringType != typeof(ITimePeriodBuilder))
            {
                ThrowInvalidMethodArgument(lambda, method);
                return null;
            }

            var duration = ((ConstantExpression)mce.Arguments[0]).Value;
            return $"{duration} {mce.Method.Name}";
        }

        private static void ThrowInvalidMethodArgument(Expression argument, string method)
        {
            throw new InvalidOperationException($"Invalid '{method}' argument: '{argument}'");
        }
    }

}
