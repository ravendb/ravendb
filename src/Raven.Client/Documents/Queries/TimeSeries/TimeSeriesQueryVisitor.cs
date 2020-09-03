using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    internal class TimeSeriesQueryVisitor<T>
    {
        private readonly RavenQueryProviderProcessor<T> _providerProcessor;
        private readonly IAbstractDocumentQuery<T> _documentQuery;
        private readonly LinqPathProvider _linqPathProvider;


        private TimeSeriesWhereClauseVisitor<T> _whereVisitor;
        private StringBuilder _selectFields;
        private string _src, _between, _where, _groupBy, _last, _first, _loadTag, _offset, _scale;

        public List<string> Parameters { get; internal set; }

        public TimeSeriesQueryVisitor(RavenQueryProviderProcessor<T> processor) : 
            this(processor.DocumentQuery, processor.LinqPathProvider)
        {
            _providerProcessor = processor;
        }

        internal TimeSeriesQueryVisitor(IAbstractDocumentQuery<T> documentQuery, LinqPathProvider linqPathProvider)
        {
            _documentQuery = documentQuery;
            _linqPathProvider = linqPathProvider;
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
                case nameof(ITimeSeriesQueryable.Offset):
                    Offset(mce.Arguments[0]);
                    break;
                case nameof(ITimeSeriesQueryable.Scale):
                    Scale(mce.Arguments[0]);
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

            _whereVisitor = new TimeSeriesWhereClauseVisitor<T>(lambda.Parameters[0].Name, _documentQuery);

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
            string with = null;

            if (expression is ConstantExpression constantExpression)
            {
                if (constantExpression.Value is Action<ITimePeriodBuilder> action)
                {
                    // document query
                    timePeriod = GetTimePeriodFromAction(action, out with);
                }
                else
                {
                    timePeriod = constantExpression.Value.ToString();
                }
            }

            else if (expression is LambdaExpression lambda)
            {
                if (!(lambda.Body is MethodCallExpression mce))
                {
                    ThrowInvalidMethodArgument(lambda, nameof(ITimeSeriesQueryable.GroupBy));
                    return;
                }

                MethodCallExpression groupByCall;
                if (mce.Object is MethodCallExpression innerCall)
                {
                    groupByCall = innerCall;
                    with = GroupByWith(mce, lambda);
                }
                else
                {
                    groupByCall = mce;
                }

                timePeriod = GetTimePeriodFromMethodCall(groupByCall, nameof(ITimeSeriesQueryable.GroupBy));
            }

            else
            {
                timePeriod = expression.ToString();
            }

            _groupBy = $" group by '{timePeriod}' {with}";
        }

        private static string GetTimePeriodFromAction(Action<ITimePeriodBuilder> action, out string with)
        {
            with = null;

            var builder = new TimePeriodBuilder();
            action.Invoke(builder);

            var timePeriod = builder.GetTimePeriod();

            if (builder.Options != null && builder.Options.Interpolation != InterpolationType.None)
            {
                with = $"with interpolation({builder.Options.Interpolation})";
            }

            return timePeriod;
        }

        private static string GroupByWith(MethodCallExpression callExpression, Expression expression)
        {
            if (callExpression.Method.DeclaringType != typeof(ITimeSeriesAggregationOperations))
            {
                ThrowInvalidMethodArgument(expression, nameof(ITimeSeriesQueryable.GroupBy));
                return null;
            }

            if (callExpression.Method.Name != nameof(ITimeSeriesAggregationOperations.WithOptions))
                throw new NotSupportedException("Unsupported method in GroupBy clause: " + callExpression.Method.Name);

            LinqPathProvider.GetValueFromExpressionWithoutConversion(callExpression.Arguments[0], out var value);
            if (!(value is TimeSeriesAggregationOptions options))
            {
                ThrowInvalidMethodArgument(callExpression.Arguments[0], nameof(ITimeSeriesAggregationOperations.WithOptions));
                return null;
            }

            if (options.Interpolation == InterpolationType.None) 
                return null;

            return $"with interpolation({options.Interpolation})";

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

        private void Scale(Expression expression)
        {
            if (expression.Type != typeof(double))
                throw new InvalidOperationException($"Invalid argument in method '{nameof(ITimeSeriesQueryable.Scale)}'. " +
                                                    $"Expected argument of type 'double' but got : '{expression.Type}'");

            LinqPathProvider.GetValueFromExpressionWithoutConversion(expression, out var value);

            _scale = $" scale {value}";
        }

        private void TimeSeriesCall(MethodCallExpression mce)
        {
            if (mce.Arguments.Count == 1)
            {
                _src = GetNameFromArgument(mce.Arguments[0]);
            }
            else
            {
                _src = GetNameFromArgument(mce.Arguments[1]);

                if (!(mce.Arguments[0] is ConstantExpression constant))
                    throw new InvalidOperationException(); //todo

                if (constant.Value != null)
                {
                    var sourceAlias = LinqPathProvider.RemoveTransparentIdentifiersIfNeeded(mce.Arguments[0].ToString());

                    if (_providerProcessor != null)
                    {
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
                    }

                    if (mce.Arguments[1] is ParameterExpression == false)
                        _src = $"{sourceAlias}.{_src}";
                }

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

            var timePeriod = GetTimePeriodFromExpression(expression, nameof(ITimeSeriesQueryable.FromLast));

            _last = $" last {timePeriod}";
        }

        private static string GetTimePeriodFromExpression(Expression expression, string method)
        {
            if (expression is ConstantExpression constant &&
                constant.Value is Action<ITimePeriodBuilder> action)
            {
                // document query
                return GetTimePeriodFromAction(action, out _);
            }
            
            if (!(expression is LambdaExpression lambda) ||
                !(lambda.Body is MethodCallExpression methodCall))
            {
                ThrowInvalidMethodArgument(expression, method);
                return null;
            }

            return GetTimePeriodFromMethodCall(methodCall, method);
        }

        private void First(Expression expression)
        {
            if (_last != null)
                throw new InvalidQueryException($"Cannot use both '{nameof(ITimeSeriesQueryable.FromFirst)}' and '{nameof(ITimeSeriesQueryable.FromLast)}' in the same Time Series query function ");

            var timePeriod = GetTimePeriodFromExpression(expression, nameof(ITimeSeriesQueryable.FromFirst));

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
                from = _documentQuery.ProjectionParameter(from);

            if (!(mce.Arguments[3] is ParameterExpression))
                to = _documentQuery.ProjectionParameter(to);

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

            var objects = (IEnumerable)_linqPathProvider.GetValueFromExpression(mce.Arguments[1], typeof(IEnumerable));

            var parameter = _documentQuery.ProjectionParameter(objects);

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
            if (_scale != null)
                queryBuilder.Append(_scale);
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
            {
                if (constant.Value is DateTime dt)
                    return dt.EnsureUtc().GetDefaultRavenFormat();

                return constant.Value.ToString();
            }

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

        private static string GetTimePeriodFromMethodCall(MethodCallExpression callExpression, string method)
        {
            if (callExpression.Method.DeclaringType != typeof(ITimePeriodBuilder))
            {
                ThrowInvalidMethodArgument(callExpression, method);
                return null;
            }

            var duration = ((ConstantExpression)callExpression.Arguments[0]).Value;
            return $"{duration} {callExpression.Method.Name}";
        }

        private static void ThrowInvalidMethodArgument(Expression argument, string method)
        {
            throw new InvalidOperationException($"Invalid '{method}' argument: '{argument}'");
        }

        internal class TimePeriodBuilder : ITimePeriodBuilder, ITimeSeriesAggregationOperations
        {
            private int _duration;
            private string _methodName;

            public TimeSeriesAggregationOptions Options { get; private set; }

            public string GetTimePeriod()
            {
                return $"{_duration} {_methodName}";
            }

            public ITimeSeriesAggregationOperations Milliseconds(int duration)
            {
                _duration = duration;
                _methodName = nameof(Milliseconds);
                return this;
            }

            public ITimeSeriesAggregationOperations Seconds(int duration)
            {
                _duration = duration;
                _methodName = nameof(Seconds);
                return this;
            }

            public ITimeSeriesAggregationOperations Minutes(int duration)
            {
                _duration = duration;
                _methodName = nameof(Minutes);
                return this;
            }

            public ITimeSeriesAggregationOperations Hours(int duration)
            {
                _duration = duration;
                _methodName = nameof(Hours);
                return this;
            }

            public ITimeSeriesAggregationOperations Days(int duration)
            {
                _duration = duration;
                _methodName = nameof(Days);
                return this;
            }

            public ITimeSeriesAggregationOperations Months(int duration)
            {
                _duration = duration;
                _methodName = nameof(Months);
                return this;
            }

            public ITimeSeriesAggregationOperations Quarters(int duration)
            {
                _duration = duration;
                _methodName = nameof(Quarters);
                return this;
            }

            public ITimeSeriesAggregationOperations Years(int duration)
            {
                _duration = duration;
                _methodName = nameof(Years);
                return this;
            }

            public void WithOptions(TimeSeriesAggregationOptions options)
            {
                Options = options;
            }
        }
    }

}
