//-----------------------------------------------------------------------
// <copyright file="RavenQueryProviderProcessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Linq
{
    /// <summary>
    /// Process a Linq expression to a Lucene query
    /// </summary>
    internal class RavenQueryProviderProcessor<T>
    {
        private readonly Action<IDocumentQueryCustomization> _customizeQuery;
        /// <summary>
        /// The query generator
        /// </summary>
        protected readonly IDocumentQueryGenerator QueryGenerator;
        private readonly Action<QueryResult> _afterQueryExecuted;
        private bool _chainedWhere;
        private int _insideWhere;
        private bool _insideExact;
        private IAbstractDocumentQuery<T> _documentQuery;
        private SpecialQueryType _queryType = SpecialQueryType.None;
        private Type _newExpressionType;
        private string _currentPath = string.Empty;
        private int _subClauseDepth;
        private readonly string _resultsTransformer;
        private readonly Parameters _transformerParameters;
        private Expression _groupByElementSelector;

        private readonly LinqPathProvider _linqPathProvider;
        /// <summary>
        /// The index name
        /// </summary>
        protected readonly string IndexName;

        private readonly string _collectionName;

        /// <summary>
        /// Gets the current path in the case of expressions within collections
        /// </summary>
        public string CurrentPath => _currentPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryProviderProcessor{T}"/> class.
        /// </summary>
        /// <param name="queryGenerator">The document query generator.</param>
        /// <param name="customizeQuery">The customize query.</param>
        /// <param name="afterQueryExecuted">Executed after the query run, allow access to the query results</param>
        /// <param name="indexName">The name of the index the query is executed against.</param>
        /// <param name="collectionName">The name of the collection the query is executed against.</param>
        /// <param name="fieldsToFetch">The fields to fetch in this query</param>
        /// <param name="isMapReduce"></param>
        /// <param name="resultsTransformer"></param>
        /// <param name="transformerParameters"></param>
        /// /// <param name ="originalType" >the original type of the query if TransformWith is called otherwise null</param>
        public RavenQueryProviderProcessor(IDocumentQueryGenerator queryGenerator, Action<IDocumentQueryCustomization> customizeQuery, Action<QueryResult> afterQueryExecuted,
             string indexName, string collectionName, HashSet<FieldToFetch> fieldsToFetch, bool isMapReduce, string resultsTransformer,
            Parameters transformerParameters, Type originalType)
        {
            FieldsToFetch = fieldsToFetch;
            _newExpressionType = typeof(T);
            QueryGenerator = queryGenerator;
            IndexName = indexName;
            _collectionName = collectionName;
            _isMapReduce = isMapReduce;
            _afterQueryExecuted = afterQueryExecuted;
            _customizeQuery = customizeQuery;
            _resultsTransformer = resultsTransformer;
            _transformerParameters = transformerParameters;
            _originalQueryType = originalType ?? throw new ArgumentNullException(nameof(originalType));
            _linqPathProvider = new LinqPathProvider(queryGenerator.Conventions);
        }

        /// <summary>
        /// Gets or sets the fields to fetch.
        /// </summary>
        /// <value>The fields to fetch.</value>
        public HashSet<FieldToFetch> FieldsToFetch { get; set; }

        /// <summary>
        /// Visits the expression and generate the lucene query
        /// </summary>
        /// <param name="expression">The expression.</param>
        protected void VisitExpression(Expression expression)
        {
            if (expression is BinaryExpression)
            {
                VisitBinaryExpression((BinaryExpression)expression);
            }
            else
            {
                switch (expression.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        VisitMemberAccess((MemberExpression)expression, true);
                        break;
                    case ExpressionType.Not:
                        var unaryExpressionOp = ((UnaryExpression)expression).Operand;
                        switch (unaryExpressionOp.NodeType)
                        {
                            case ExpressionType.MemberAccess:
                                VisitMemberAccess((MemberExpression)unaryExpressionOp, false);
                                break;
                            case ExpressionType.Call:
                                // probably a call to !In() or !string.IsNullOrEmpty()
                                _documentQuery.OpenSubclause();
                                _documentQuery.WhereTrue();
                                _documentQuery.AndAlso();
                                _documentQuery.NegateNext();
                                VisitMethodCall((MethodCallExpression)unaryExpressionOp, negated: true);
                                _documentQuery.CloseSubclause();
                                break;
                            default:
                                //probably the case of !(complex condition)
                                _documentQuery.OpenSubclause();
                                _documentQuery.WhereTrue();
                                _documentQuery.AndAlso();
                                _documentQuery.NegateNext();
                                VisitExpression(unaryExpressionOp);
                                _documentQuery.CloseSubclause();
                                break;
                        }
                        break;
                    case ExpressionType.Convert:
                    case ExpressionType.ConvertChecked:
                        VisitExpression(((UnaryExpression)expression).Operand);
                        break;
                    default:
                        if (expression is MethodCallExpression)
                        {
                            VisitMethodCall((MethodCallExpression)expression);
                        }
                        else
                        {
                            var lambdaExpression = expression as LambdaExpression;
                            if (lambdaExpression != null)
                            {
                                var body = lambdaExpression.Body;
                                if (body.NodeType == ExpressionType.Constant && ((ConstantExpression)body).Value is bool)
                                {
                                    throw new ArgumentException("Constants expressions such as Where(x => true) are not allowed in the RavenDB queries");
                                }
                                VisitExpression(body);
                            }
                        }
                        break;
                }
            }
        }

        private void VisitBinaryExpression(BinaryExpression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.OrElse:
                    VisitOrElse(expression);
                    break;
                case ExpressionType.AndAlso:
                    VisitAndAlso(expression);
                    break;
                case ExpressionType.NotEqual:
                    VisitNotEquals(expression);
                    break;
                case ExpressionType.Equal:
                    VisitEquals(expression);
                    break;
                case ExpressionType.GreaterThan:
                    VisitGreaterThan(expression);
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    VisitGreaterThanOrEqual(expression);
                    break;
                case ExpressionType.LessThan:
                    VisitLessThan(expression);
                    break;
                case ExpressionType.LessThanOrEqual:
                    VisitLessThanOrEqual(expression);
                    break;
            }

        }

        private void VisitAndAlso(BinaryExpression andAlso)
        {
            if (TryHandleBetween(andAlso))
                return;


            if (_subClauseDepth > 0)
                _documentQuery.OpenSubclause();
            _subClauseDepth++;

            // negate optimization : (RavenDB-3973).  in order to disable you may just set isNotEqualCheckBoundsToAndAlsoLeft & Right to "false" 
            bool isNotEqualCheckBoundsToAndAlsoLeft = (andAlso.Left.NodeType == ExpressionType.NotEqual);
            bool isNotEqualCheckBoundsToAndAlsoRight = (andAlso.Right.NodeType == ExpressionType.NotEqual);

            if (isNotEqualCheckBoundsToAndAlsoRight && isNotEqualCheckBoundsToAndAlsoLeft)
                // avoid empty group (i.e. : "a != 1 && a != 2"  should generate "((-a:1 AND a:*) AND -a:2)"
                isNotEqualCheckBoundsToAndAlsoLeft = false;

            if (isNotEqualCheckBoundsToAndAlsoLeft || isNotEqualCheckBoundsToAndAlsoRight)
            {
                _subClauseDepth++;
                _documentQuery.OpenSubclause();
            }
            _isNotEqualCheckBoundsToAndAlso = isNotEqualCheckBoundsToAndAlsoLeft;
            VisitExpression(andAlso.Left);
            _documentQuery.AndAlso();
            _isNotEqualCheckBoundsToAndAlso = isNotEqualCheckBoundsToAndAlsoRight;
            VisitExpression(andAlso.Right);
            _isNotEqualCheckBoundsToAndAlso = false;

            if (isNotEqualCheckBoundsToAndAlsoLeft || isNotEqualCheckBoundsToAndAlsoRight)
            {
                _subClauseDepth--;
                _documentQuery.CloseSubclause();
            }


            _subClauseDepth--;
            if (_subClauseDepth > 0)
                _documentQuery.CloseSubclause();
        }

        private bool TryHandleBetween(BinaryExpression andAlso)
        {
            // x.Foo > 100 && x.Foo < 200
            // x.Foo < 200 && x.Foo > 100 
            // 100 < x.Foo && 200 > x.Foo
            // 200 > x.Foo && 100 < x.Foo 

            // x.Foo >= 100 && x.Foo <= 200
            // x.Foo <= 200 && x.Foo >= 100 
            // 100 <= x.Foo && 200 >= x.Foo
            // 200 >= x.Foo && 100 <= x.Foo

            var isPossibleBetween =
                (andAlso.Left.NodeType == ExpressionType.GreaterThan && andAlso.Right.NodeType == ExpressionType.LessThan) ||
                (andAlso.Left.NodeType == ExpressionType.GreaterThanOrEqual && andAlso.Right.NodeType == ExpressionType.LessThanOrEqual) ||
                (andAlso.Left.NodeType == ExpressionType.LessThan && andAlso.Right.NodeType == ExpressionType.GreaterThan) ||
                (andAlso.Left.NodeType == ExpressionType.LessThanOrEqual && andAlso.Right.NodeType == ExpressionType.GreaterThanOrEqual);

            if (isPossibleBetween == false)
                return false;

            var leftMember = GetMemberForBetween((BinaryExpression)andAlso.Left);
            var rightMember = GetMemberForBetween((BinaryExpression)andAlso.Right);

            if (leftMember == null || rightMember == null)
                return false;

            // both must be on the same property
            if (leftMember.Item1.Path != rightMember.Item1.Path)
                return false;

            var min = (andAlso.Left.NodeType == ExpressionType.LessThan ||
                       andAlso.Left.NodeType == ExpressionType.LessThanOrEqual)
                          ? rightMember.Item2
                          : leftMember.Item2;
            var max = (andAlso.Left.NodeType == ExpressionType.LessThan ||
                       andAlso.Left.NodeType == ExpressionType.LessThanOrEqual)
                          ? leftMember.Item2
                          : rightMember.Item2;

            if (andAlso.Left.NodeType == ExpressionType.GreaterThanOrEqual || andAlso.Left.NodeType == ExpressionType.LessThanOrEqual)
                _documentQuery.WhereBetween(leftMember.Item1.Path, min, max, _insideExact);
            else
            {
                _documentQuery.WhereGreaterThan(leftMember.Item1.Path, min, _insideExact);
                _documentQuery.AndAlso();
                _documentQuery.WhereLessThan(leftMember.Item1.Path, max, _insideExact);
            }

            return true;
        }

        private Tuple<ExpressionInfo, object> GetMemberForBetween(BinaryExpression binaryExpression)
        {
            if (IsMemberAccessForQuerySource(binaryExpression.Left))
            {
                var expressionInfo = GetMember(binaryExpression.Left);
                return Tuple.Create(expressionInfo, GetValueFromExpression(binaryExpression.Right, expressionInfo.Type));
            }
            if (IsMemberAccessForQuerySource(binaryExpression.Right))
            {
                var expressionInfo = GetMember(binaryExpression.Right);
                return Tuple.Create(expressionInfo, GetValueFromExpression(binaryExpression.Left, expressionInfo.Type));
            }
            return null;
        }

        private object GetValueFromExpression(Expression expression, Type type)
        {
            return _linqPathProvider.GetValueFromExpression(expression, type);
        }

        private void VisitOrElse(BinaryExpression orElse)
        {
            if (_subClauseDepth > 0)
                _documentQuery.OpenSubclause();
            _subClauseDepth++;

            VisitExpression(orElse.Left);
            _documentQuery.OrElse();
            VisitExpression(orElse.Right);

            _subClauseDepth--;
            if (_subClauseDepth > 0)
                _documentQuery.CloseSubclause();
        }

        private void VisitEquals(BinaryExpression expression)
        {
            var constantExpression = expression.Right as ConstantExpression;
            if (constantExpression != null && true.Equals(constantExpression.Value))
            {
                VisitExpression(expression.Left);
                return;
            }


            if (constantExpression != null && false.Equals(constantExpression.Value) &&
                expression.Left.NodeType != ExpressionType.MemberAccess)
            {
                _documentQuery.OpenSubclause();
                _documentQuery.WhereTrue();
                _documentQuery.AndAlso();
                _documentQuery.NegateNext();
                VisitExpression(expression.Left);
                _documentQuery.CloseSubclause();
                return;
            }

            var methodCallExpression = expression.Left as MethodCallExpression;
            // checking for VB.NET string equality
            if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" &&
                expression.Right.NodeType == ExpressionType.Constant &&
                Equals(((ConstantExpression)expression.Right).Value, 0))
            {
                var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);

                _documentQuery.WhereEquals(
                    new WhereParams
                    {
                        FieldName = expressionMemberInfo.Path,
                        Value = GetValueFromExpression(methodCallExpression.Arguments[1], GetMemberType(expressionMemberInfo)),
                        AllowWildcards = false,
                        Exact = _insideExact
                    });
                return;
            }

            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitEquals(Expression.Equal(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);

            _documentQuery.WhereEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
                AllowWildcards = false,
                IsNestedPath = memberInfo.IsNestedPath,
                Exact = _insideExact
            });
        }

        private bool IsMemberAccessForQuerySource(Expression node)
        {
            while (node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
            {
                node = ((UnaryExpression)node).Operand;
            }
            if (node.NodeType == ExpressionType.Parameter)
                return true;
            if (node.NodeType != ExpressionType.MemberAccess)
                return false;
            var memberExpression = ((MemberExpression)node);
            if (memberExpression.Expression == null) // static call
                return false;
            if (memberExpression.Expression.NodeType == ExpressionType.Constant)
                return false;
            return IsMemberAccessForQuerySource(memberExpression.Expression);
        }

        private void VisitNotEquals(BinaryExpression expression)
        {
            var methodCallExpression = expression.Left as MethodCallExpression;
            // checking for VB.NET string equality
            if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" &&
                expression.Right.NodeType == ExpressionType.Constant &&
                Equals(((ConstantExpression)expression.Right).Value, 0))
            {
                var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
                _documentQuery.OpenSubclause();
                _documentQuery.WhereExists(expressionMemberInfo.Path);
                _documentQuery.AndAlso();
                _documentQuery.NegateNext();
                _documentQuery.WhereEquals(new WhereParams
                {
                    FieldName = expressionMemberInfo.Path,
                    Value = GetValueFromExpression(methodCallExpression.Arguments[0], GetMemberType(expressionMemberInfo)),
                    AllowWildcards = false,
                    Exact = _insideExact
                });
                _documentQuery.CloseSubclause();
                return;
            }

            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitNotEquals(Expression.NotEqual(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            if (_isNotEqualCheckBoundsToAndAlso == false)
            {
                _documentQuery.OpenSubclause();
                _documentQuery.WhereExists(memberInfo.Path);
                _documentQuery.AndAlso();
            }

            _documentQuery.NegateNext();
            _documentQuery.WhereEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
                AllowWildcards = false,
                Exact = _insideExact
            });

            if (_isNotEqualCheckBoundsToAndAlso == false)
                _documentQuery.CloseSubclause();
        }

        private static Type GetMemberType(ExpressionInfo info)
        {
            return info.Type;
        }

        private static readonly Regex CastingRemover = new Regex(@"(?<!\\)[\(\)]", RegexOptions.Compiled);
        private static readonly Regex ConvertRemover = new Regex(@"^(Convert\((.+)\,.*\))", RegexOptions.Compiled);

        /// <summary>
        /// Gets member info for the specified expression and the path to that expression
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        protected virtual ExpressionInfo GetMember(Expression expression)
        {
            var parameterExpression = GetParameterExpressionIncludingConvertions(expression);
            if (parameterExpression != null)
            {
                if (_currentPath.EndsWith("[]."))
                    _currentPath = _currentPath.Substring(0, _currentPath.Length - 3);
                return new ExpressionInfo(_currentPath, parameterExpression.Type, false);
            }

            return GetMemberDirect(expression);
        }

        private ExpressionInfo GetMemberDirect(Expression expression)
        {
            var result = _linqPathProvider.GetPath(expression);

            //for standard queries, we take just the last part. But for dynamic queries, we take the whole part

            var convertMatch = ConvertRemover.Match(result.Path);
            if (convertMatch.Success)
                result.Path = result.Path.Replace(convertMatch.Groups[1].Value, convertMatch.Groups[2].Value);
            result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);
            result.Path = CastingRemover.Replace(result.Path, string.Empty); // removing cast remains

            if (expression.NodeType == ExpressionType.ArrayLength)
                result.Path += ".Length";

            var propertyName = IndexName == null && _collectionName != null
                                   ? QueryGenerator.Conventions.FindPropertyNameForDynamicIndex(typeof(T), IndexName, CurrentPath,
                                                                                                result.Path)
                                   : QueryGenerator.Conventions.FindPropertyNameForIndex(typeof(T), IndexName, CurrentPath,
                                                                                         result.Path);
            return new ExpressionInfo(propertyName, result.MemberType, result.IsNestedPath)
            {
                MaybeProperty = result.MaybeProperty
            };
        }

        private static ParameterExpression GetParameterExpressionIncludingConvertions(Expression expression)
        {
            var paramExpr = expression as ParameterExpression;
            if (paramExpr != null)
                return paramExpr;
            switch (expression.NodeType)
            {
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return GetParameterExpressionIncludingConvertions(((UnaryExpression)expression).Operand);
            }
            return null;
        }


        private void VisitEquals(MethodCallExpression expression)
        {
            ExpressionInfo fieldInfo = null;
            Expression constant = null;
            object comparisonType = null;

            if (expression.Object == null)
            {
                var a = expression.Arguments[0];
                var b = expression.Arguments[1];

                if (a is MemberExpression && b is ConstantExpression)
                {
                    fieldInfo = GetMember(a);
                    constant = b;
                }
                else if (a is ConstantExpression && b is MemberExpression)
                {
                    fieldInfo = GetMember(b);
                    constant = a;
                }

                if (expression.Arguments.Count == 3 &&
                    expression.Arguments[2].NodeType == ExpressionType.Constant &&
                    expression.Arguments[2].Type == typeof(StringComparison))
                {
                    comparisonType = ((ConstantExpression)expression.Arguments[2]).Value;

                }
            }
            else
            {
                switch (expression.Object.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        fieldInfo = GetMember(expression.Object);
                        constant = expression.Arguments[0];
                        break;
                    case ExpressionType.Constant:
                        fieldInfo = GetMember(expression.Arguments[0]);
                        constant = expression.Object;
                        break;
                    case ExpressionType.Parameter:
                        fieldInfo = new ExpressionInfo(_currentPath.Substring(0, _currentPath.Length - 1), expression.Object.Type,
                                                       false);
                        constant = expression.Arguments[0];
                        break;
                    default:
                        throw new NotSupportedException("Nodes of type + " + expression.Object.NodeType + " are not understood in this context");
                }
                if (expression.Arguments.Count == 2 &&
                    expression.Arguments[1].NodeType == ExpressionType.Constant &&
                    expression.Arguments[1].Type == typeof(StringComparison))
                {
                    comparisonType = ((ConstantExpression)expression.Arguments[1]).Value;
                }
            }

            if (comparisonType != null)
            {
                switch ((StringComparison)comparisonType)
                {
                    case StringComparison.CurrentCulture:
                    case StringComparison.Ordinal:
                        throw new NotSupportedException(
                            "RavenDB queries case sensitivity is dependent on the index, not the query. If you need case sensitive queries, use a static index and an NotAnalyzed field for that.");
                    case StringComparison.CurrentCultureIgnoreCase:
                    case StringComparison.OrdinalIgnoreCase:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }



            _documentQuery.WhereEquals(new WhereParams
            {
                FieldName = fieldInfo.Path,
                Value = GetValueFromExpression(constant, GetMemberType(fieldInfo)),
                AllowWildcards = false,
                Exact = _insideExact
            });
        }

        private void VisitStringContains(MethodCallExpression _)
        {
            throw new NotSupportedException(@"Contains is not supported, doing a substring match over a text field is a very slow operation, and is not allowed using the Linq API.
The recommended method is to use full text search (mark the field as Analyzed and use the Search() method to query it.");
        }

        private void VisitStartsWith(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Object);

            _documentQuery.WhereStartsWith(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
        }

        private void VisitEndsWith(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Object);

            _documentQuery.WhereEndsWith(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
        }

        private void VisitIsNullOrEmpty(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);

            _documentQuery.OpenSubclause();
            _documentQuery.WhereEquals(memberInfo.Path, null, _insideExact);
            _documentQuery.OrElse();
            _documentQuery.WhereEquals(memberInfo.Path, string.Empty, _insideExact);
            _documentQuery.CloseSubclause();
        }

        private void VisitGreaterThan(BinaryExpression expression)
        {
            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitLessThan(Expression.LessThan(expression.Right, expression.Left));
                return;
            }
            var memberInfo = GetMember(expression.Left);
            var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

            _documentQuery.WhereGreaterThan(
                GetFieldNameForRangeQuery(memberInfo, value),
                value,
                _insideExact);
        }

        private void VisitGreaterThanOrEqual(BinaryExpression expression)
        {
            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitLessThanOrEqual(Expression.LessThanOrEqual(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);

            var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

            _documentQuery.WhereGreaterThanOrEqual(
                GetFieldNameForRangeQuery(memberInfo, value),
                value,
                _insideExact);
        }

        private void VisitLessThan(BinaryExpression expression)
        {
            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitGreaterThan(Expression.GreaterThan(expression.Right, expression.Left));
                return;
            }
            var memberInfo = GetMember(expression.Left);
            var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

            _documentQuery.WhereLessThan(
                GetFieldNameForRangeQuery(memberInfo, value),
                value,
                _insideExact);
        }

        private void VisitLessThanOrEqual(BinaryExpression expression)
        {
            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitGreaterThanOrEqual(Expression.GreaterThanOrEqual(expression.Right, expression.Left));
                return;
            }
            var memberInfo = GetMember(expression.Left);


            var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

            _documentQuery.WhereLessThanOrEqual(
                GetFieldNameForRangeQuery(memberInfo, value),
                value,
                _insideExact);
        }

        private void VisitAny(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);
            if (expression.Arguments.Count >= 2)
            {
                var oldPath = _currentPath;
                _currentPath = memberInfo.Path + "[].";
                VisitExpression(expression.Arguments[1]);
                _currentPath = oldPath;
            }
            else
            {
                // Support for .Where(x => x.Properties.Any())
                _documentQuery.WhereExists(memberInfo.Path);
            }
        }

        private void VisitContains(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);
            var containsArgument = expression.Arguments[1];

            _documentQuery.WhereEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(containsArgument, containsArgument.Type),
                AllowWildcards = false,
                Exact = _insideExact
            });

        }

        private void VisitMemberAccess(MemberExpression memberExpression, bool boolValue)
        {
            if (memberExpression.Type == typeof(bool))
            {
                ExpressionInfo memberInfo;
                if (memberExpression.Member.Name == "HasValue" &&
                    Nullable.GetUnderlyingType(memberExpression.Member.DeclaringType) != null)
                {
                    memberInfo = GetMember(memberExpression.Expression);
                    if (boolValue)
                    {
                        _documentQuery.OpenSubclause();
                        _documentQuery.WhereTrue();
                        _documentQuery.AndAlso();
                        _documentQuery.NegateNext();
                    }
                    _documentQuery.WhereEquals(new WhereParams
                    {
                        FieldName = memberInfo.Path,
                        Value = null,
                        AllowWildcards = false,
                        Exact = _insideExact
                    });
                    if (boolValue)
                    {
                        _documentQuery.CloseSubclause();
                    }
                }
                else
                {
                    memberInfo = GetMember(memberExpression);

                    _documentQuery.WhereEquals(new WhereParams
                    {
                        FieldName = memberInfo.Path,
                        Value = boolValue,
                        AllowWildcards = false,
                        Exact = _insideExact
                    });
                }
            }
            else if (memberExpression.Type == typeof(string))
            {
                if (_currentPath.EndsWith("[]."))
                    _currentPath = _currentPath.Substring(0, _currentPath.Length - 1);

                var memberInfo = GetMember(memberExpression);

                _documentQuery.WhereEquals(new WhereParams
                {
                    FieldName = _currentPath,
                    Value = GetValueFromExpression(memberExpression, GetMemberType(memberInfo)),
                    AllowWildcards = false,
                    IsNestedPath = memberInfo.IsNestedPath,
                    Exact = _insideExact
                });
            }
            else
            {
                throw new NotSupportedException("Expression type not supported: " + memberExpression);
            }
        }

        private void VisitMethodCall(MethodCallExpression expression, bool negated = false)
        {
            var declaringType = expression.Method.DeclaringType;
            Debug.Assert(declaringType != null);
            if (declaringType != typeof(string) && expression.Method.Name == "Equals")
            {
                switch (expression.Arguments.Count)
                {
                    case 1:
                        VisitEquals(Expression.MakeBinary(ExpressionType.Equal, expression.Object, expression.Arguments[0]));
                        break;
                    case 2:
                        VisitEquals(Expression.MakeBinary(ExpressionType.Equal, expression.Arguments[0], expression.Arguments[1]));
                        break;
                    default:
                        throw new ArgumentException("Can't understand Equals with " + expression.Arguments.Count + " arguments");
                }
                return;
            }
            if (declaringType == typeof(LinqExtensions) ||
                declaringType == typeof(RavenQueryableExtensions))
            {
                VisitLinqExtensionsMethodCall(expression);
                return;
            }
            if (declaringType == typeof(Queryable))
            {
                VisitQueryableMethodCall(expression);
                return;
            }

            if (declaringType == typeof(String))
            {
                VisitStringMethodCall(expression);
                return;
            }

            if (declaringType == typeof(Enumerable))
            {
                VisitEnumerableMethodCall(expression, negated);
                return;
            }

            if (declaringType.GetTypeInfo().IsGenericType)
            {
                var genericTypeDefinition = declaringType.GetGenericTypeDefinition();
                if (genericTypeDefinition == typeof(ICollection<>) ||
                    genericTypeDefinition == typeof(List<>) ||
                    genericTypeDefinition == typeof(IList<>) ||
                    genericTypeDefinition == typeof(Array))
                {
                    VisitListMethodCall(expression);
                    return;
                }
            }

            var method = declaringType.Name + "." + expression.Method.Name;
            throw new NotSupportedException(string.Format("Method not supported: {0}. Expression: {1}.", method, expression));
        }

        private void VisitLinqExtensionsMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "Search":
                    VisitSearch(expression);
                    break;
                case "OrderByScore":
                    _documentQuery.OrderByScore();
                    VisitExpression(expression.Arguments[0]);
                    break;
                case "OrderByScoreDescending":
                    _documentQuery.OrderByScoreDescending();
                    VisitExpression(expression.Arguments[0]);
                    break;
                case "Intersect":
                    VisitExpression(expression.Arguments[0]);
                    _documentQuery.Intersect();
                    _chainedWhere = false;
                    break;
                case "In":
                    var memberInfo = GetMember(expression.Arguments[0]);
                    var objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    _documentQuery.WhereIn(memberInfo.Path, ((IEnumerable)objects).Cast<object>(), _insideExact);
                    break;
                case "ContainsAny":
                    memberInfo = GetMember(expression.Arguments[0]);
                    objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    _documentQuery.ContainsAny(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
                    break;
                case "ContainsAll":
                    memberInfo = GetMember(expression.Arguments[0]);
                    objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    _documentQuery.ContainsAll(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
                    break;
                case nameof(LinqExtensions.Where):
                    VisitQueryableMethodCall(expression);
                    break;
                default:
                    {
                        throw new NotSupportedException("Method not supported: " + expression.Method.Name);
                    }
            }
        }

        private void VisitSearch(MethodCallExpression searchExpression)
        {
            var expressions = new List<MethodCallExpression>();

            var search = searchExpression;
            var target = searchExpression.Arguments[0];
            object value;
            while (true)
            {

                expressions.Add(search);

                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(search.Arguments[4], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract value from " + searchExpression);
                }
                var queryOptions = (SearchOptions)value;
                if (queryOptions.HasFlag(SearchOptions.Guess) == false)
                    break;

                search = search.Arguments[0] as MethodCallExpression;
                if (search == null ||
                    search.Method.Name != "Search" ||
                    search.Method.DeclaringType != typeof(LinqExtensions))
                    break;

                target = search.Arguments[0];
            }

            VisitExpression(target);

            if (expressions.Count > 1)
            {
                _documentQuery.OpenSubclause();
            }

            foreach (var expression in Enumerable.Reverse(expressions))
            {
                var expressionInfo = GetMember(expression.Arguments[1]);
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract value from " + expression);
                }
                var searchTerms = (string)value;
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[3], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract value from " + expression);
                }
                var boost = (decimal)value;
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[4], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract value from " + expression);
                }
                var options = (SearchOptions)value;
                if (_chainedWhere && options.HasFlag(SearchOptions.And))
                {
                    _documentQuery.AndAlso();
                }
                if (options.HasFlag(SearchOptions.Not))
                {
                    _documentQuery.OpenSubclause();
                    _documentQuery.WhereExists(expressionInfo.Path);
                    _documentQuery.AndAlso();
                    _documentQuery.NegateNext();
                }

                _documentQuery.Search(expressionInfo.Path, searchTerms);
                if (options.HasFlag(SearchOptions.Not))
                {
                    _documentQuery.CloseSubclause();
                }

                _documentQuery.Boost(boost);

                if (options.HasFlag(SearchOptions.And))
                {
                    _chainedWhere = true;
                }
            }

            if (expressions.Count > 1)
            {
                _documentQuery.CloseSubclause();
            }

            if (LinqPathProvider.GetValueFromExpressionWithoutConversion(searchExpression.Arguments[4], out value) == false)
            {
                throw new InvalidOperationException("Could not extract value from " + searchExpression);
            }

            if (((SearchOptions)value).HasFlag(SearchOptions.Guess))
                _chainedWhere = true;
        }

        private void VisitListMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "Contains":
                    {
                        var memberInfo = GetMember(expression.Object);

                        var containsArgument = expression.Arguments[0];

                        _documentQuery.WhereEquals(new WhereParams
                        {
                            FieldName = memberInfo.Path,
                            Value = GetValueFromExpression(containsArgument, containsArgument.Type),
                            AllowWildcards = false,
                            Exact = _insideExact
                        });

                        break;
                    }
                default:
                    {
                        throw new NotSupportedException("Method not supported: List." + expression.Method.Name);
                    }
            }
        }

        private void VisitEnumerableMethodCall(MethodCallExpression expression, bool negated)
        {
            switch (expression.Method.Name)
            {
                case "Any":
                    {
                        if (negated)
                            throw new InvalidOperationException("Cannot process negated Any(), see RavenDB-732 http://issues.hibernatingrhinos.com/issue/RavenDB-732");

                        if (expression.Arguments.Count == 1 && expression.Arguments[0].Type == typeof(string))
                        {
                            _documentQuery.OpenSubclause();
                            _documentQuery.WhereTrue();
                            _documentQuery.AndAlso();
                            _documentQuery.NegateNext();
                            VisitIsNullOrEmpty(expression);
                            _documentQuery.CloseSubclause();
                        }
                        else
                        {
                            VisitAny(expression);
                        }
                        break;
                    }
                case "Contains":
                    {
                        if (expression.Arguments.First().Type == typeof(string))
                        {
                            VisitStringContains(expression);
                        }
                        else
                        {
                            VisitContains(expression);
                        }
                        break;
                    }
                default:
                    {
                        throw new NotSupportedException("Method not supported: " + expression.Method.Name);
                    }
            }
        }

        private void VisitStringMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "Contains":
                    {
                        VisitStringContains(expression);
                        break;
                    }
                case "Equals":
                    {
                        VisitEquals(expression);
                        break;
                    }
                case "StartsWith":
                    {
                        VisitStartsWith(expression);
                        break;
                    }
                case "EndsWith":
                    {
                        VisitEndsWith(expression);
                        break;
                    }
                case "IsNullOrEmpty":
                    {
                        VisitIsNullOrEmpty(expression);
                        break;
                    }
                default:
                    {
                        throw new NotSupportedException("Method not supported: " + expression.Method.Name);
                    }
            }
        }

        private void VisitQueryableMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "OfType":
                    VisitExpression(expression.Arguments[0]);
                    break;
                case "Where":
                    {
                        _insideWhere++;
                        VisitExpression(expression.Arguments[0]);
                        if (_chainedWhere)
                        {
                            _documentQuery.AndAlso();
                            _documentQuery.OpenSubclause();
                        }
                        if (_chainedWhere == false && _insideWhere > 1)
                            _documentQuery.OpenSubclause();

                        if (expression.Arguments.Count == 3)
                            _insideExact = (bool)GetValueFromExpression(expression.Arguments[2], typeof(bool));

                        VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);

                        _insideExact = false;

                        if (_chainedWhere == false && _insideWhere > 1)
                            _documentQuery.CloseSubclause();
                        if (_chainedWhere)
                            _documentQuery.CloseSubclause();
                        _chainedWhere = true;
                        _insideWhere--;
                        break;
                    }
                case "Select":
                    {
                        if (expression.Arguments[0].Type.GetTypeInfo().IsGenericType &&
                                expression.Arguments[0].Type.GetGenericTypeDefinition() == typeof(IQueryable<>) &&
                            expression.Arguments[0].Type != expression.Arguments[1].Type)
                        {
                            _documentQuery.AddRootType(expression.Arguments[0].Type.GetGenericArguments()[0]);
                        }
                        VisitExpression(expression.Arguments[0]);
                        var operand = ((UnaryExpression)expression.Arguments[1]).Operand;

                        if (_documentQuery.IsDynamicMapReduce)
                        {
                            VisitSelectAfterGroupBy(operand, _groupByElementSelector);
                            _groupByElementSelector = null;
                        }
                        else
                            VisitSelect(operand);
                        break;
                    }
                case "Skip":
                    {
                        VisitExpression(expression.Arguments[0]);
                        VisitSkip(((ConstantExpression)expression.Arguments[1]));
                        break;
                    }
                case "Take":
                    {
                        VisitExpression(expression.Arguments[0]);
                        VisitTake(((ConstantExpression)expression.Arguments[1]));
                        break;
                    }
                case "First":
                case "FirstOrDefault":
                    {
                        VisitExpression(expression.Arguments[0]);
                        if (expression.Arguments.Count == 2)
                        {
                            if (_chainedWhere)
                                _documentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        }

                        if (expression.Method.Name == "First")
                        {
                            VisitFirst();
                        }
                        else
                        {
                            VisitFirstOrDefault();
                        }
                        _chainedWhere = _chainedWhere || expression.Arguments.Count == 2;
                        break;
                    }
                case "Single":
                case "SingleOrDefault":
                    {
                        VisitExpression(expression.Arguments[0]);
                        if (expression.Arguments.Count == 2)
                        {
                            if (_chainedWhere)
                                _documentQuery.AndAlso();

                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        }

                        if (expression.Method.Name == "Single")
                        {
                            VisitSingle();
                        }
                        else
                        {
                            VisitSingleOrDefault();
                        }
                        _chainedWhere = _chainedWhere || expression.Arguments.Count == 2;
                        break;
                    }
                case "All":
                    {
                        VisitExpression(expression.Arguments[0]);
                        VisitAll((Expression<Func<T, bool>>)((UnaryExpression)expression.Arguments[1]).Operand);
                        break;
                    }
                case "Any":
                    {
                        VisitExpression(expression.Arguments[0]);
                        if (expression.Arguments.Count == 2)
                        {
                            if (_chainedWhere)
                                _documentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        }

                        VisitAny();
                        break;
                    }
                case "Count":
                    {
                        VisitExpression(expression.Arguments[0]);
                        if (expression.Arguments.Count == 2)
                        {
                            if (_chainedWhere)
                                _documentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        }

                        VisitCount();
                        break;
                    }
                case "LongCount":
                    {
                        VisitExpression(expression.Arguments[0]);
                        if (expression.Arguments.Count == 2)
                        {
                            if (_chainedWhere)
                                _documentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        }

                        VisitLongCount();
                        break;
                    }
                case "Distinct":
                    if (expression.Arguments.Count == 1)
                    {
                        _documentQuery.Distinct();
                        VisitExpression(expression.Arguments[0]);
                        break;
                    }
                    throw new NotSupportedException("Method not supported: Distinct(IEqualityComparer<T>)");
                case "OrderBy":
                case "ThenBy":
                case "ThenByDescending":
                case "OrderByDescending":
                    VisitExpression(expression.Arguments[0]);
                    VisitOrderBy((LambdaExpression)((UnaryExpression)expression.Arguments[1]).Operand,
                                 expression.Method.Name.EndsWith("Descending"));
                    break;
                case "GroupBy":
                    if (_documentQuery.CollectionName == null)
                        throw new NotSupportedException("GroupBy method is only supported in dynamic map-reduce queries");

                    if (expression.Arguments.Count == 5) // GroupBy(x => keySelector, x => elementSelector, x => resultSelector, IEqualityComparer)
                        throw new NotSupportedException("Dynamic map-reduce queries does not support a custom equality comparer");

                    VisitExpression(expression.Arguments[0]);
                    VisitGroupBy(((UnaryExpression)expression.Arguments[1]).Operand);

                    if (expression.Arguments.Count >= 3)
                    {
                        var lambdaExpression = ((UnaryExpression)expression.Arguments[2]).Operand as LambdaExpression;

                        if (lambdaExpression == null)
                            throw new NotSupportedException("Expected lambda expression as a element selector in GroupBy statement");

                        Expression elementSelector = lambdaExpression.Body as MemberExpression; // x => x.Property

                        if (elementSelector == null)
                            elementSelector = lambdaExpression.Body as MethodCallExpression; // x.Collection.AggregatingFunction(y => y.Property)

                        if (expression.Arguments.Count == 3) // GroupBy(x => keySelector, x => elementSelector)
                            _groupByElementSelector = elementSelector;
                        else if (expression.Arguments.Count == 4) // GroupBy(x => keySelector, x => elementSelector, x => resultSelector)
                            VisitSelectAfterGroupBy(((UnaryExpression)expression.Arguments[3]).Operand, elementSelector);
                        else
                            throw new NotSupportedException($"Not supported syntax of GroupBy. Number of arguments: {expression.Arguments.Count}");
                    }

                    break;
                default:
                    {
                        throw new NotSupportedException("Method not supported: " + expression.Method.Name);
                    }
            }
        }

        private void VisitGroupBy(Expression expression)
        {
            var lambdaExpression = expression as LambdaExpression;

            if (lambdaExpression == null)
                throw new NotSupportedException("We expect GroupBy statement to have lambda expression");

            var body = lambdaExpression.Body;
            switch (body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var singleGroupByFieldName = GetSelectPath(_linqPathProvider.GetMemberExpression(lambdaExpression));

                    _documentQuery.GroupBy(singleGroupByFieldName);
                    break;
                case ExpressionType.New:
                    var newExpression = ((NewExpression)body);

                    for (int index = 0; index < newExpression.Arguments.Count; index++)
                    {
                        var originalField = GetSelectPath((MemberExpression)newExpression.Arguments[index]);

                        _documentQuery.GroupBy(originalField);

                        AddGroupByAliasIfNeeded(newExpression.Members[index], originalField);
                    }
                    break;
                case ExpressionType.MemberInit:
                    var memberInitExpression = ((MemberInitExpression)body);

                    for (int index = 0; index < memberInitExpression.Bindings.Count; index++)
                    {
                        var field = memberInitExpression.Bindings[index] as MemberAssignment;

                        if (field == null)
                            throw new InvalidOperationException($"We expected MemberAssignment expression while got {memberInitExpression.Bindings[index].GetType().FullName} in GroupBy");

                        var originalField = GetSelectPath((MemberExpression)field.Expression);

                        _documentQuery.GroupBy(originalField);

                        AddGroupByAliasIfNeeded(field.Member, originalField);
                    }
                    break;
                default:
                    throw new NotSupportedException("Node not supported in GroupBy: " + body.NodeType);

            }
        }

        private void AddGroupByAliasIfNeeded(MemberInfo aliasMember, string originalField)
        {
            var alias = GetSelectPath(aliasMember);

            if (alias != null && originalField.Equals(alias, StringComparison.Ordinal) == false)
            {
                if (_documentQuery is DocumentQuery<T> docQuery)
                    docQuery.AddGroupByAlias(originalField, alias);
                else if (_documentQuery is AsyncDocumentQuery<T> asyncDocQuery)
                    asyncDocQuery.AddGroupByAlias(originalField, alias);
            }
        }

        private void VisitOrderBy(LambdaExpression expression, bool descending)
        {
            var result = GetMemberDirect(expression.Body);

            var fieldType = result.Type;
            var fieldName = result.Path;
            if (result.MaybeProperty != null &&
                QueryGenerator.Conventions.FindIdentityProperty(result.MaybeProperty))
            {
                fieldName = Constants.Documents.Indexing.Fields.DocumentIdFieldName;
                fieldType = typeof(string);
            }

            _documentQuery.AddOrder(fieldName, descending, OrderingUtil.GetOrderingOfType(fieldType));
        }

        private bool _insideSelect;
        private readonly bool _isMapReduce;
        private readonly Type _originalQueryType;
        private bool _isNotEqualCheckBoundsToAndAlso;

        private void VisitSelect(Expression operand)
        {
            var lambdaExpression = operand as LambdaExpression;
            var body = lambdaExpression != null ? lambdaExpression.Body : operand;
            switch (body.NodeType)
            {
                case ExpressionType.Convert:
                    _insideSelect = true;
                    try
                    {
                        VisitSelect(((UnaryExpression)body).Operand);
                    }
                    finally
                    {
                        _insideSelect = false;
                    }
                    break;
                case ExpressionType.MemberAccess:
                    var memberExpression = ((MemberExpression)body);
                    AddToFieldsToFetch(GetSelectPath(memberExpression), GetSelectPath(memberExpression));
                    if (_insideSelect == false)
                    {
                        foreach (var fieldToFetch in FieldsToFetch)
                        {
                            if (fieldToFetch.Name != memberExpression.Member.Name)
                                continue;

                            fieldToFetch.Alias = null;
                        }
                    }
                    break;
                //Anonymous types come through here .Select(x => new { x.Cost } ) doesn't use a member initializer, even though it looks like it does
                //See http://blogs.msdn.com/b/sreekarc/archive/2007/04/03/immutable-the-new-anonymous-type.aspx
                case ExpressionType.New:
                    var newExpression = ((NewExpression)body);
                    _newExpressionType = newExpression.Type;

                    for (int index = 0; index < newExpression.Arguments.Count; index++)
                    {
                        var field = newExpression.Arguments[index] as MemberExpression;
                        if (field == null)
                            continue;
                        var expression = _linqPathProvider.GetMemberExpression(newExpression.Arguments[index]);
                        AddToFieldsToFetch(GetSelectPath(expression), GetSelectPath(newExpression.Members[index]));
                    }
                    break;
                //for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
                case ExpressionType.MemberInit:
                    var memberInitExpression = ((MemberInitExpression)body);
                    _newExpressionType = memberInitExpression.NewExpression.Type;
                    foreach (MemberBinding t in memberInitExpression.Bindings)
                    {
                        var field = t as MemberAssignment;
                        if (field == null)
                            continue;

                        var expression = _linqPathProvider.GetMemberExpression(field.Expression);
                        var renamedField = GetSelectPath(expression);

                        AddToFieldsToFetch(renamedField, GetSelectPath(field.Member));
                    }
                    break;
                case ExpressionType.Parameter: // want the full thing, so just pass it on.
                    break;
                //for example .Select(product => product.Properties["VendorName"])
                case ExpressionType.Call:
                    var expressionInfo = GetMember(body);
                    AddToFieldsToFetch(expressionInfo.Path, null);
                    break;

                default:
                    throw new NotSupportedException("Node not supported: " + body.NodeType);
            }
        }

        private void VisitSelectAfterGroupBy(Expression operand, Expression elementSelectorPath)
        {
            if (_documentQuery.IsDynamicMapReduce == false)
                throw new NotSupportedException("Expected a query to be a dynamic map reduce query");

            var lambdaExpression = (LambdaExpression)operand;
            var body = lambdaExpression.Body;

            switch (body.NodeType)
            {
                //Anonymous types come through here .Select(x => new { x.Cost } ) doesn't use a member initializer, even though it looks like it does
                //See http://blogs.msdn.com/b/sreekarc/archive/2007/04/03/immutable-the-new-anonymous-type.aspx
                case ExpressionType.New:
                    var newExpression = ((NewExpression)body);
                    _newExpressionType = newExpression.Type;

                    for (int index = 0; index < newExpression.Arguments.Count; index++)
                    {
                        HandleOutputFieldOfDynamicMapReduce(lambdaExpression, newExpression.Arguments[index], newExpression.Members[index], elementSelectorPath);
                    }
                    break;
                //for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
                case ExpressionType.MemberInit:
                    var memberInitExpression = ((MemberInitExpression)body);
                    _newExpressionType = memberInitExpression.NewExpression.Type;
                    foreach (MemberBinding t in memberInitExpression.Bindings)
                    {
                        var field = (MemberAssignment)t;

                        HandleOutputFieldOfDynamicMapReduce(lambdaExpression, field.Expression, field.Member, elementSelectorPath);
                    }
                    break;
                default:
                    throw new NotSupportedException("Node not supported in Select after GroupBy: " + body.NodeType);
            }
        }

        private void HandleOutputFieldOfDynamicMapReduce(LambdaExpression entireExpression, Expression fieldExpression, MemberInfo fieldMember, Expression elementSelectorPath)
        {
            switch (fieldExpression.NodeType)
            {
                case ExpressionType.Parameter:
                    var parameterExpression = (ParameterExpression)fieldExpression; // GroupBy(x => key, x => element, (parameter, g) => new { Name = parameter, ... })

                    if (entireExpression.Parameters.Count != 2)
                        throw new NotSupportedException($"Lambda with {entireExpression.Parameters.Count} parameters is not supported inside GroupBy");

                    if (entireExpression.Parameters[0].Name == parameterExpression.Name)
                    {
                        _documentQuery.GroupByKey(null, GetSelectPath(fieldMember));
                    }
                    break;
                case ExpressionType.MemberAccess:

                    var keyExpression = (MemberExpression)fieldExpression;
                    var name = GetSelectPath(keyExpression);

                    if ("Key".Equals(name, StringComparison.Ordinal))
                    {
                        var projectedName = ExtractProjectedName(fieldMember);

                        if (projectedName.Equals("Key", StringComparison.Ordinal))
                            _documentQuery.GroupByKey(null);
                        else
                            _documentQuery.GroupByKey(null, projectedName);
                    }
                    else if (name.StartsWith("Key.", StringComparison.Ordinal))
                    {
                        var compositeGroupBy = name.Split('.');

                        if (compositeGroupBy.Length > 2)
                            throw new NotSupportedException("Nested fields inside composite GroupBy keys are not supported");

                        var fieldName = compositeGroupBy[1];
                        var projectedName = ExtractProjectedName(fieldMember);

                        _documentQuery.GroupByKey(fieldName, projectedName);
                    }
                    break;
                case ExpressionType.Call:
                    var mapReduceOperationCall = (MethodCallExpression)fieldExpression;

                    AddMapReduceField(mapReduceOperationCall, fieldMember, elementSelectorPath);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported node type inside Select following GroupBy: {fieldExpression.NodeType}");
            }
        }

        private static string ExtractProjectedName(MemberInfo fieldMember)
        {
            if (fieldMember == null)
                return null;

            return GetSelectPath(fieldMember);
        }

        private void AddMapReduceField(MethodCallExpression mapReduceOperationCall, MemberInfo memberInfo, Expression elementSelectorPath)
        {
            if (mapReduceOperationCall.Method.DeclaringType != typeof(Enumerable))
                throw new NotSupportedException($"Unsupported method in select of dynamic map reduce query: {mapReduceOperationCall.Method.Name} of type {mapReduceOperationCall.Method.DeclaringType}");

            AggregationOperation mapReduceOperation;
            if (Enum.TryParse(mapReduceOperationCall.Method.Name, out mapReduceOperation) == false)
                throw new NotSupportedException($"Unhandled map reduce operation type: {mapReduceOperationCall.Method.Name}");

            string renamedField = null;
            string mapReduceField;

            if (mapReduceOperationCall.Arguments.Count == 1)
            {
                if (elementSelectorPath == null)
                    mapReduceField = GetSelectPath(memberInfo);
                else
                {
                    mapReduceField = GetSelectPath(elementSelectorPath as MemberExpression);
                    renamedField = GetSelectPath(memberInfo);
                }
            }
            else
            {
                renamedField = GetSelectPath(memberInfo);

                var lambdaExpression = mapReduceOperationCall.Arguments[1] as LambdaExpression;

                if (lambdaExpression == null)
                    throw new NotSupportedException("Expected lambda expression in Select statement of a dynamic map-reduce query");

                var member = lambdaExpression.Body as MemberExpression;

                if (member == null)
                    member = elementSelectorPath as MemberExpression;

                if (member != null)
                {
                    mapReduceField = GetSelectPath(member);
                }
                else
                {
                    // x.Collection.AggregatingFunction(y => y.Property) syntax

                    var methodCallExpression = lambdaExpression.Body as MethodCallExpression;

                    if (methodCallExpression == null)
                        methodCallExpression = elementSelectorPath as MethodCallExpression;

                    if (methodCallExpression == null)
                        throw new NotSupportedException("No idea how to handle this dynamic map-reduce query!");

                    switch (methodCallExpression.Method.Name)
                    {
                        case "Sum":
                            {
                                if (mapReduceOperation != AggregationOperation.Sum)
                                    throw new NotSupportedException("Cannot use different aggregating functions for a single field");

                                if (methodCallExpression.Arguments.Count != 2)
                                    throw new NotSupportedException($"Incompatible number of arguments of Sum function: {methodCallExpression.Arguments.Count}");

                                var firstPart = GetMember(methodCallExpression.Arguments[0]);
                                var secondPart = GetMember(methodCallExpression.Arguments[1]);

                                mapReduceField = $"{firstPart.Path}[].{secondPart.Path}";

                                break;
                            }
                        default:
                            {
                                throw new NotSupportedException("Method not supported: " + methodCallExpression.Method.Name);
                            }
                    }
                }
            }

            if (mapReduceOperation == AggregationOperation.Count)
            {
                if (mapReduceField.Equals("Count", StringComparison.Ordinal))
                    _documentQuery.GroupByCount();
                else
                    _documentQuery.GroupByCount(mapReduceField);
                return;
            }

            if (mapReduceOperation == AggregationOperation.Sum)
            {
                _documentQuery.GroupBySum(mapReduceField, renamedField);
                return;
            }

            throw new NotSupportedException($"Map-Reduce operation '{mapReduceOperation}' in '{mapReduceOperationCall}' is not supported.");
        }

        private static string GetSelectPath(MemberInfo member)
        {
            return LinqPathProvider.HandlePropertyRenames(member, member.Name);
        }

        private string GetSelectPath(MemberExpression expression)
        {
            var expressionInfo = GetMember(expression);
            return expressionInfo.Path;
        }

        private void AddToFieldsToFetch(string field, string alias)
        {
            var identityProperty = _documentQuery.Conventions.GetIdentityProperty(_originalQueryType);
            if (identityProperty != null && identityProperty.Name == field)
            {
                FieldsToFetch.Add(new FieldToFetch(Constants.Documents.Indexing.Fields.DocumentIdFieldName, alias));
                return;
            }

            FieldsToFetch.Add(new FieldToFetch(field, alias));
        }

        private void VisitSkip(ConstantExpression constantExpression)
        {
            //Don't have to worry about the cast failing, the Skip() extension method only takes an int
            _documentQuery.Skip((int)constantExpression.Value);
        }

        private void VisitTake(ConstantExpression constantExpression)
        {
            //Don't have to worry about the cast failing, the Take() extension method only takes an int
            _documentQuery.Take((int)constantExpression.Value);
        }

        private void VisitAll(Expression<Func<T, bool>> predicateExpression)
        {
            throw new NotSupportedException("All() is not supported for linq queries");
        }

        private void VisitAny()
        {
            _documentQuery.Take(1);
            _queryType = SpecialQueryType.Any;
        }

        private void VisitCount()
        {
            _documentQuery.Take(0);
            _queryType = SpecialQueryType.Count;
        }

        private void VisitLongCount()
        {
            _documentQuery.Take(0);
            _queryType = SpecialQueryType.LongCount;
        }

        private void VisitSingle()
        {
            _documentQuery.Take(2);
            _queryType = SpecialQueryType.Single;
        }

        private void VisitSingleOrDefault()
        {
            _documentQuery.Take(2);
            _queryType = SpecialQueryType.SingleOrDefault;
        }

        private void VisitFirst()
        {
            _documentQuery.Take(1);
            _queryType = SpecialQueryType.First;
        }

        private void VisitFirstOrDefault()
        {
            _documentQuery.Take(1);
            _queryType = SpecialQueryType.FirstOrDefault;
        }

        private string GetFieldNameForRangeQuery(ExpressionInfo expression, object value)
        {
            var identityProperty = _documentQuery.Conventions.GetIdentityProperty(typeof(T));
            if (identityProperty != null && identityProperty.Name == expression.Path)
            {
                if (identityProperty.Type() == typeof(int) ||
                    identityProperty.Type() == typeof(long) ||
                    identityProperty.Type() == typeof(double) ||
                    identityProperty.Type() == typeof(float) ||
                    identityProperty.Type() == typeof(Guid) ||
                    identityProperty.Type() == typeof(decimal))
                {
                    throw new NotSupportedException("You cannot issue range queries on a identity property that is of a numeric type.\r\n" +
                                                    "RavenDB numeric ids are purely client side, on the server, they are always strings, " +
                                                    "and aren't going to sort according to your expectations.\r\n" +
                                                    "You can create a stand-in property to hold the numeric value, and do a range query on that.");
                }
                return Constants.Documents.Indexing.Fields.DocumentIdFieldName;
            }

            return expression.Path;
        }

        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        public IDocumentQuery<T> GetDocumentQueryFor(Expression expression)
        {
            var documentQuery = QueryGenerator.Query<T>(IndexName, _collectionName, _isMapReduce);
            _documentQuery = (IAbstractDocumentQuery<T>)documentQuery;

            try
            {
                VisitExpression(expression);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("Could not understand expression: " + expression, e);
            }
            catch (NotSupportedException e)
            {
                throw new NotSupportedException("Could not understand expression: " + expression, e);
            }

            _customizeQuery?.Invoke((IDocumentQueryCustomization)_documentQuery);

            var (fields, projections) = GetProjections();

            return documentQuery.SelectFields<T>(fields, projections);
        }

        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        public IAsyncDocumentQuery<T> GetAsyncDocumentQueryFor(Expression expression)
        {
            var asyncDocumentQuery = QueryGenerator.AsyncQuery<T>(IndexName, _collectionName, _isMapReduce);
            _documentQuery = (IAbstractDocumentQuery<T>)asyncDocumentQuery;
            try
            {
                VisitExpression(expression);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("Could not understand expression: " + expression, e);
            }
            catch (NotSupportedException e)
            {
                throw new NotSupportedException("Could not understand expression: " + expression, e);
            }

            _customizeQuery?.Invoke((IDocumentQueryCustomization)asyncDocumentQuery);

            var (fields, projections) = GetProjections();

            return asyncDocumentQuery.SelectFields<T>(fields, projections);
        }

        /// <summary>
        /// Executes the specified expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public object Execute(Expression expression)
        {
            _chainedWhere = false;

            _documentQuery = (IAbstractDocumentQuery<T>)GetDocumentQueryFor(expression);
            if (_newExpressionType == typeof(T))
                return ExecuteQuery<T>();

            var genericExecuteQuery = typeof(RavenQueryProviderProcessor<T>).GetMethod("ExecuteQuery", BindingFlags.Instance | BindingFlags.NonPublic);
            var executeQueryWithProjectionType = genericExecuteQuery.MakeGenericMethod(_newExpressionType);
            return executeQueryWithProjectionType.Invoke(this, new object[0]);
        }

        internal (string[] Fields, string[] Projections) GetProjections()
        {
            var fields = new string[FieldsToFetch.Count];
            var projections = new string[FieldsToFetch.Count];

            var i = 0;
            foreach (var fieldToFetch in FieldsToFetch)
            {
                fields[i] = fieldToFetch.Name;
                projections[i] = fieldToFetch.Alias ?? fieldToFetch.Name;

                i++;
            }

            return (fields, projections);
        }

        private object ExecuteQuery<TProjection>()
        {
            var (fields, projections) = GetProjections();

            var finalQuery = ((IDocumentQuery<T>)_documentQuery).SelectFields<TProjection>(fields, projections);

            //no reason to override a value that may or may not exist there
            if (!String.IsNullOrEmpty(_resultsTransformer))
            {
            }

            var executeQuery = GetQueryResult(finalQuery);

            var queryResult = finalQuery.QueryResult;
            _afterQueryExecuted?.Invoke(queryResult);
            return executeQuery;
        }

        private object GetQueryResult<TProjection>(IDocumentQuery<TProjection> finalQuery)
        {
            switch (_queryType)
            {
                case SpecialQueryType.First:
                    {
                        return finalQuery.First();
                    }
                case SpecialQueryType.FirstOrDefault:
                    {
                        return finalQuery.FirstOrDefault();
                    }
                case SpecialQueryType.Single:
                    {
                        return finalQuery.Single();
                    }
                case SpecialQueryType.SingleOrDefault:
                    {
                        return finalQuery.SingleOrDefault();
                    }
                case SpecialQueryType.Any:
                    {
                        return finalQuery.Any();
                    }
                case SpecialQueryType.Count:
                case SpecialQueryType.LongCount:
                    {
                        if (finalQuery.IsDistinct)
                            throw new NotSupportedException("RavenDB does not support mixing Distinct & Count together.\r\n" +
                                                            "See: https://groups.google.com/forum/#!searchin/ravendb/CountDistinct/ravendb/yKQikUYKY5A/nCNI5oQB700J");
                        var qr = finalQuery.QueryResult;
                        if (_queryType != SpecialQueryType.Count)
                            return (long)qr.TotalResults;
                        return qr.TotalResults;
                    }
                default:
                    {
                        return finalQuery;
                    }
            }
        }

        #region Nested type: SpecialQueryType

        /// <summary>
        /// Different query types 
        /// </summary>
        protected enum SpecialQueryType
        {
            /// <summary>
            /// 
            /// </summary>
            None,
            /// <summary>
            /// 
            /// </summary>
            Any,
            /// <summary>
            /// Get count of items for the query
            /// </summary>
            Count,
            /// <summary>
            /// Get count of items for the query as an Int64
            /// </summary>
            LongCount,
            /// <summary>
            /// Get only the first item
            /// </summary>
            First,
            /// <summary>
            /// Get only the first item (or null)
            /// </summary>
            FirstOrDefault,
            /// <summary>
            /// Get only the first item (or throw if there are more than one)
            /// </summary>
            Single,
            /// <summary>
            /// Get only the first item (or throw if there are more than one) or null if empty
            /// </summary>
            SingleOrDefault
        }

        #endregion
    }

    public class FieldToFetch
    {
        public FieldToFetch(string name, string alias)
        {
            Name = name;
            Alias = name != alias ? alias : null;
        }

        public string Name { get; }
        public string Alias { get; internal set; }

        protected bool Equals(FieldToFetch other)
        {
            return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase) && string.Equals(Alias, other.Alias, StringComparison.Ordinal);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((FieldToFetch)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0) * 397) ^ (Alias != null ? StringComparer.Ordinal.GetHashCode(Alias) : 0);
            }
        }
    }
}
