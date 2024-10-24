//-----------------------------------------------------------------------
// <copyright file="RavenQueryProviderProcessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Lambda2Js;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Util;
using MethodCallExpression = System.Linq.Expressions.MethodCallExpression;
using NotSupportedException = System.NotSupportedException;

namespace Raven.Client.Documents.Linq
{
    /// <summary>
    /// Process a Linq expression to a Lucene query
    /// </summary>
    internal sealed class RavenQueryProviderProcessor<T>
    {
        private readonly Action<IDocumentQueryCustomization> _customizeQuery;
        /// <summary>
        /// The query generator
        /// </summary>
        private readonly IDocumentQueryGenerator QueryGenerator;

        internal const string WholeDocumentComparisonExceptionMessage =
            "You cannot compare the whole document in Where closure. Please write a conditional statement using fields from the document. A query such as session.Query<User>().Where(u => u == null) is not meaningful, you are asserting that the document itself is not null, which it can never be. Did you intend to assert that a property isn't set to null?";
        internal IAbstractDocumentQuery<T> DocumentQuery;
        internal string FromAlias;
        internal readonly LinqPathProvider LinqPathProvider;
        private readonly Action<QueryResult> _afterQueryExecuted;
        private readonly LinqQueryHighlightings _highlightings;
        private readonly QueryStatistics _queryStatistics;
        private bool _chainedWhere;
        private int _insideWhereOrSearchCounter;
        private int _insideFilterCounter;
        private bool _insideNegate;
        private bool _insideExact;
        private SpecialQueryType _queryType = SpecialQueryType.None;
        private Type _newExpressionType;
        private string _currentPath = string.Empty;
        private int _subClauseDepth;
        private Expression _groupByElementSelector;
        private string _jsSelectBody;
        private List<string> _jsProjectionNames;
        private StringBuilder _declareBuilder;
        private List<DeclareToken> _declareTokens;
        private List<LoadToken> _loadTokens;
        private HashSet<string> _loadAliases;
        private HashSet<Type> _loadTypes;
        private readonly HashSet<string> _loadAliasesMovedToOutputFunction;
        private int _insideLet = 0;
        private string _loadAlias;
        private bool _selectLoad;
        private const string DefaultLoadAlias = "__load";
        private const string DefaultAliasPrefix = "__alias";
        private bool _addedDefaultAlias;
        private Type _ofType;
        private bool _isSelectArg;
        private (bool IsSet, string Name) _manualLet;
        private JavascriptConversionExtensions.TypedParameterSupport _typedParameterSupport;

        private readonly HashSet<string> _aliasKeywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "AS",
            "SELECT",
            "WHERE",
            "GROUP",
            "ORDER",
            "INCLUDE",
            "UPDATE",
            "LIMIT",
            "OFFSET"
        };

        private List<string> _projectionParameters;

        private int _aliasesCount;

        /// <summary>
        /// The index name
        /// </summary>
        private readonly string IndexName;

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
        /// /// <param name ="originalType" >the original type of the query if TransformWith is called otherwise null</param>
        public RavenQueryProviderProcessor(
            IDocumentQueryGenerator queryGenerator,
            Action<IDocumentQueryCustomization> customizeQuery,
            Action<QueryResult> afterQueryExecuted,
            LinqQueryHighlightings highlightings,
             string indexName,
            string collectionName,
            HashSet<FieldToFetch> fieldsToFetch,
            bool isMapReduce,
            Type originalType,
            DocumentConventions conventions,
            bool isProjectInto,
            QueryStatistics queryStatistics)
        {
            FieldsToFetch = fieldsToFetch;
            _newExpressionType = typeof(T);
            QueryGenerator = queryGenerator;
            IndexName = indexName;
            _collectionName = collectionName;
            _isMapReduce = isMapReduce;
            _afterQueryExecuted = afterQueryExecuted;
            _highlightings = highlightings;
            _customizeQuery = customizeQuery;
            _originalQueryType = originalType ?? throw new ArgumentNullException(nameof(originalType));
            _conventions = conventions;
            _isProjectInto = isProjectInto;
            _queryStatistics = queryStatistics;
            LinqPathProvider = new LinqPathProvider(queryGenerator.Conventions);
            _jsProjectionNames = new List<string>();
            _loadAliasesMovedToOutputFunction = new HashSet<string>();
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
        private void VisitExpression(Expression expression)
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
                                DocumentQuery.OpenSubclause();
                                DocumentQuery.WhereTrue();
                                DocumentQuery.AndAlso();
                                DocumentQuery.NegateNext();
                                VisitMethodCall((MethodCallExpression)unaryExpressionOp);
                                DocumentQuery.CloseSubclause();
                                break;
                            default:
                                //probably the case of !(complex condition)
                                DocumentQuery.OpenSubclause();
                                DocumentQuery.WhereTrue();
                                DocumentQuery.AndAlso();
                                DocumentQuery.NegateNext();
                                _insideNegate = true;
                                VisitExpression(unaryExpressionOp);
                                _insideNegate = false;
                                DocumentQuery.CloseSubclause();
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
                        else if (expression is LambdaExpression lambdaExpression)
                        {
                            var body = lambdaExpression.Body;
                            if (body.NodeType == ExpressionType.Constant && ((ConstantExpression)body).Value is bool)
                            {
                                throw new ArgumentException("Constants expressions such as Where(x => true) are not allowed in the RavenDB queries");
                            }
                            if (body.NodeType == ExpressionType.Invoke && body.Type == typeof(bool))
                            {
                                throw new NotSupportedException("Invocation expressions such as Where(x => SomeFunction(x)) are not supported in RavenDB queries");
                            }
                            VisitExpression(body);
                        }
                        break;
                }
            }
        }

        private void VisitBinaryExpression(BinaryExpression expression)
        {
            if (_insideWhereOrSearchCounter > 0)
            {
                VerifyLegalBinaryExpression(expression);
            }

            var subclause = false;
            if (_insideNegate)
            { 
                _insideNegate = false;
                subclause = true;

                DocumentQuery.OpenSubclause();
            }

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

            if (subclause)
                DocumentQuery.CloseSubclause();
        }

        private void VerifyLegalBinaryExpression(BinaryExpression expression)
        {
            if (expression.Left is BinaryExpression left)
            {
                VerifyLegalBinaryExpression(left);
            }

            if (expression.Right is BinaryExpression right)
            {
                VerifyLegalBinaryExpression(right);
            }

            switch (expression.NodeType)
            {
                case ExpressionType.OrElse:
                case ExpressionType.AndAlso:
                    return;
            }
            if (IsMemberAccessForQuerySource(expression.Left) &&
                IsMemberAccessForQuerySource(expression.Right))
            {
                // x.Foo < x.Bar
                throw new NotSupportedException("Where clauses containing a Binary Expression between two fields are not supported. " +
                                                "All Binary Expressions inside a Where clause should be between a field and a constant value. " +
                                                $"`{expression.Left}` and `{expression.Right}` are both fields, so cannot convert {expression} to a proper query.");
            }

        }

        private void VisitAndAlso(BinaryExpression andAlso)
        {
            if (TryHandleBetween(andAlso))
                return;

            if (_subClauseDepth > 0)
                DocumentQuery.OpenSubclause();
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
                DocumentQuery.OpenSubclause();
            }

            VisitExpression(andAlso.Left);
            DocumentQuery.AndAlso();
            VisitExpression(andAlso.Right);

            if (isNotEqualCheckBoundsToAndAlsoLeft || isNotEqualCheckBoundsToAndAlsoRight)
            {
                _subClauseDepth--;
                DocumentQuery.CloseSubclause();
            }

            _subClauseDepth--;
            if (_subClauseDepth > 0)
                DocumentQuery.CloseSubclause();
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

            if (isPossibleBetween == false || IsFilterModeActive())
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
                DocumentQuery.WhereBetween(leftMember.Item1.Path, min, max, _insideExact);
            else
            {
                DocumentQuery.WhereGreaterThan(leftMember.Item1.Path, min, _insideExact);
                DocumentQuery.AndAlso();
                DocumentQuery.WhereLessThan(leftMember.Item1.Path, max, _insideExact);
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
            return LinqPathProvider.GetValueFromExpression(expression, type);
        }

        private void VisitOrElse(BinaryExpression orElse)
        {
            if (_subClauseDepth > 0)
                DocumentQuery.OpenSubclause();
            _subClauseDepth++;

            VisitExpression(orElse.Left);
            DocumentQuery.OrElse();
            VisitExpression(orElse.Right);

            _subClauseDepth--;
            if (_subClauseDepth > 0)
                DocumentQuery.CloseSubclause();
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
                DocumentQuery.OpenSubclause();
                DocumentQuery.WhereTrue();
                DocumentQuery.AndAlso();
                DocumentQuery.NegateNext();
                
                _insideNegate = true;

                VisitExpression(expression.Left);

                _insideNegate = false;

                DocumentQuery.CloseSubclause();
                return;
            }

            if (expression.Left is MethodCallExpression methodCallExpression)
            {
                // checking for VB.NET string equality
                if (methodCallExpression.Method.Name == "CompareString" &&
                    expression.Right.NodeType == ExpressionType.Constant &&
                    Equals(((ConstantExpression)expression.Right).Value, 0))
                {
                    var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
                    DocumentQuery.WhereEquals(
                        new WhereParams
                        {
                            FieldName = expressionMemberInfo.Path,
                            Value = GetValueFromExpression(methodCallExpression.Arguments[1], GetMemberType(expressionMemberInfo)),
                            AllowWildcards = false,
                            Exact = _insideExact
                        });

                    return;
                }

                if (methodCallExpression.Method.DeclaringType == typeof(string))
                {
                    ExpressionInfo expressionMemberInfo = null;
                    Expression argument = null;
                    var exact = _insideExact |methodCallExpression.Method.Name == nameof(string.CompareOrdinal);

                    var isSimpleCompare = nameof(string.Compare) == methodCallExpression.Method.Name;
                    var argumentsCount = methodCallExpression.Arguments.Count;
                    
                    if (isSimpleCompare == false && argumentsCount > 2)
                        throw new NotSupportedException($"We do not support such overloads in '{methodCallExpression.Method.Name}'.");
                    if (isSimpleCompare)
                    {
                        if (argumentsCount == 3 && exact == false)
                            exact = ConvertStringComparisonToExact(methodCallExpression.Arguments[2]);
                        else if (argumentsCount > 3)
                            throw new NotSupportedException($"We do not support such overloads in '{methodCallExpression.Method.Name}'.");
                    }
                    
                    switch (methodCallExpression.Method.Name)
                    {
                        case nameof(string.Compare):
                        case nameof(string.CompareOrdinal):
                            EnsureStringComparisonMethodComparedWithZero(expression, methodCallExpression);
                            expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
                            argument = methodCallExpression.Arguments[1];
                            
                            break;
                        case nameof(string.CompareTo):
                            EnsureStringComparisonMethodComparedWithZero(expression, methodCallExpression);
                            expressionMemberInfo = GetMember(methodCallExpression.Object);
                            argument = methodCallExpression.Arguments[0];
                            break;
                    }

                    if (expressionMemberInfo != null)
                    {
                        DocumentQuery.WhereEquals(
                            new WhereParams
                            {
                                FieldName = expressionMemberInfo.Path,
                                Value = GetValueFromExpression(argument, GetMemberType(expressionMemberInfo)),
                                AllowWildcards = false,
                                Exact = exact
                            });
                        return;
                    }
                }
            }

            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitEquals(Expression.Equal(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            
            if (memberInfo.Path == string.Empty)
            {
                throw new InvalidQueryException(WholeDocumentComparisonExceptionMessage);
            }
            
            DocumentQuery.WhereEquals(new WhereParams
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
            if (node.NodeType == ExpressionType.Call && node is MethodCallExpression callExpressionNode && callExpressionNode.Method.Name == "get_Item")
            {
                if (callExpressionNode.Object?.NodeType == ExpressionType.Parameter)
                    return true;
                
                return IsMemberAccessForQuerySource(callExpressionNode.Object);
            }
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
            // checking for VB.NET string equality
            if (expression.Left is MethodCallExpression methodCallExpression) 
            {

                if (methodCallExpression.Method.Name == "CompareString" &&
                    expression.Right.NodeType == ExpressionType.Constant &&
                    Equals(((ConstantExpression)expression.Right).Value, 0))
                {
                    var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
                    DocumentQuery.WhereNotEquals(new WhereParams
                    {
                        FieldName = expressionMemberInfo.Path,
                        Value = GetValueFromExpression(methodCallExpression.Arguments[0], GetMemberType(expressionMemberInfo)),
                        AllowWildcards = false,
                        Exact = _insideExact
                    });
                    return;
                }

                if (methodCallExpression.Method.DeclaringType == typeof(string))
                {
                    ExpressionInfo expressionMemberInfo = null;
                    Expression argument = null;
                    var exact = _insideExact | methodCallExpression.Method.Name == nameof(string.CompareOrdinal);
                    switch (methodCallExpression.Method.Name)
                    {
                        case nameof(string.Compare):
                        case nameof(string.CompareOrdinal):
                            EnsureStringComparisonMethodComparedWithZero(expression, methodCallExpression);
                            expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
                            argument = methodCallExpression.Arguments[1];
                            break;
                        case nameof(string.CompareTo):
                            EnsureStringComparisonMethodComparedWithZero(expression, methodCallExpression);
                            expressionMemberInfo = GetMember(methodCallExpression.Object);
                            argument = methodCallExpression.Arguments[0];
                            break;
                    }

                    if (expressionMemberInfo != null)
                    {
                        DocumentQuery.WhereNotEquals(
                            new WhereParams
                            {
                                FieldName = expressionMemberInfo.Path,
                                Value = GetValueFromExpression(argument, GetMemberType(expressionMemberInfo)),
                                AllowWildcards = false,
                                Exact = exact
                            });
                        return;
                    }
                }
            }
            
            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitNotEquals(Expression.NotEqual(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            if (memberInfo.Path == string.Empty)
            {
                throw new InvalidQueryException(WholeDocumentComparisonExceptionMessage);
            }

            DocumentQuery.WhereNotEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
                AllowWildcards = false,
                Exact = _insideExact
            });
        }

        private static void EnsureStringComparisonMethodComparedWithZero(BinaryExpression expression, MethodCallExpression mce)
        {
            bool comparedToConstantZero = expression.Right.NodeType == ExpressionType.Constant &&
                                          Equals(((ConstantExpression)expression.Right).Value, 0);
            if (comparedToConstantZero == false)
            {
                throw new NotSupportedException("Usage of " + mce.Method + ", requires a comparison to a constant 0 only (use: string.Compare(x.Value, Arg) == 0 ), but was: " + expression);
            }
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
        private ExpressionInfo GetMember(Expression expression)
        {
            var parameterExpression = GetParameterExpressionIncludingConversions(expression);
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
            var result = LinqPathProvider.GetPath(expression, IsFilterModeActive());

            //for standard queries, we take just the last part. But for dynamic queries, we take the whole part

            var convertMatch = ConvertRemover.Match(result.Path);
            if (convertMatch.Success)
                result.Path = result.Path.Replace(convertMatch.Groups[1].Value, convertMatch.Groups[2].Value);

            var propertyName = GetPropertyName(result.Path, expression.NodeType);

            return new ExpressionInfo(propertyName, result.MemberType, result.IsNestedPath, result.Args)
            {
                MaybeProperty = result.MaybeProperty
            };
        }

        private string GetPropertyName(string selectPath, ExpressionType type)
        {
            selectPath = LinqPathProvider.RemoveTransparentIdentifiersIfNeeded(selectPath);

            var indexOf = selectPath.IndexOf('.');
            string alias = null;
            if (indexOf != -1)
            {
                alias = selectPath.Substring(0, indexOf);
                selectPath = selectPath.Substring(indexOf + 1);
            }

            selectPath = CastingRemover.Replace(selectPath, string.Empty); // removing cast remains

            if (type == ExpressionType.ArrayLength)
            {
                selectPath += ".Length";
            }

            string propertyName = null;
            if (IndexName == null && _collectionName != null)
            {
                propertyName = QueryGenerator.Conventions.FindPropertyNameForDynamicIndex(typeof(T), IndexName, CurrentPath,
                    selectPath);
            }
            else
            {
                if (_insideSelect > 0 && QueryGenerator.Conventions.FindProjectedPropertyNameForIndex != null)
                {
                    propertyName = QueryGenerator.Conventions.FindProjectedPropertyNameForIndex(typeof(T), IndexName, CurrentPath,
                        selectPath);
                }

                if (propertyName == null)
                {
                    propertyName = QueryGenerator.Conventions.FindPropertyNameForIndex(typeof(T), IndexName, CurrentPath,
                        selectPath);
                }
            }

            return AddAliasToPathIfNeeded(alias, propertyName);
        }

        private string AddAliasToPathIfNeeded(string alias, string prop)
        {
            if (alias != null &&
                (alias == FromAlias ||
                 _loadAliases != null &&
                 _loadAliases.Contains(alias)))
                return $"{alias}.{prop}";

            return prop;
        }

        private static ParameterExpression GetParameterExpressionIncludingConversions(Expression expression)
        {
            var paramExpr = expression as ParameterExpression;
            if (paramExpr != null)
                return paramExpr;
            switch (expression.NodeType)
            {
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return GetParameterExpressionIncludingConversions(((UnaryExpression)expression).Operand);
            }
            return null;
        }


        private void VisitEquals(MethodCallExpression expression)
        {
            GetStringComparisonArgumentsFromExpression(expression, out var firstArg, out var secondArg, out var comparisonArg);

            ExpressionInfo fieldInfo;
            Expression constant;
            if (IsMemberAccessForQuerySource(firstArg))
            {
                if (IsMemberAccessForQuerySource(secondArg))
                    throw new NotSupportedException("Where clauses containing an Equals Expression between two fields are not supported. " +
                                                    "The equalization should be between a field and a constant value. " +
                                                    $"`{firstArg}` and `{secondArg}` are both fields, so cannot convert {expression} to a proper query.");

                fieldInfo = GetMember(firstArg);
                constant = secondArg;
            }
            else if (IsMemberAccessForQuerySource(secondArg))
            {
                fieldInfo = GetMember(secondArg);
                constant = firstArg;
            }
            else
            {
                throw new NotSupportedException("Where clauses containing an Expression between two constants are not supported. " +
                                                "The equalization should be between a field and a constant value. " +
                                                $"`{firstArg}` and `{secondArg}` are both constants, so cannot convert {expression} to a proper query.");
            }

            bool exact = _insideExact ? true : ConvertStringComparisonToExact(comparisonArg);

            DocumentQuery.WhereEquals(new WhereParams
            {
                FieldName = fieldInfo.Path,
                Value = GetValueFromExpression(constant, GetMemberType(fieldInfo)),
                AllowWildcards = false,
                Exact = exact
            });
        }

        private static bool ConvertStringComparisonToExact(Expression comparisonArg)
        {
            if (comparisonArg != null
                    && comparisonArg.NodeType == ExpressionType.Constant
                    && comparisonArg.Type == typeof(StringComparison))
            {
                var comparisonType = ((ConstantExpression)comparisonArg).Value;
                switch ((StringComparison)comparisonType)
                {
                    case StringComparison.InvariantCulture:
                    case StringComparison.CurrentCulture:
                    case StringComparison.Ordinal:
                        return true;

                    case StringComparison.InvariantCultureIgnoreCase:
                    case StringComparison.CurrentCultureIgnoreCase:
                    case StringComparison.OrdinalIgnoreCase:
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("value", comparisonType, "Unsupported string comparison type.");
                }
            }

            return false;
        }

        private static void GetStringComparisonArgumentsFromExpression(MethodCallExpression expression, out Expression firstArg, out Expression secondArg, out Expression comparisonArg)
        {
            comparisonArg = null;
            if (expression.Object == null)
            {
                firstArg = expression.Arguments[0];
                secondArg = expression.Arguments[1];
                if (expression.Arguments.Count == 3)
                    comparisonArg = expression.Arguments[2];
            }
            else
            {
                firstArg = expression.Object;
                secondArg = expression.Arguments[0];
                if (expression.Arguments.Count == 2)
                    comparisonArg = expression.Arguments[1];
            }
        }

        private void VisitStringContains(MethodCallExpression _)
        {
            throw new NotSupportedException(@"Contains is not supported, doing a substring match over a text field is a very slow operation, and is not allowed using the Linq API.
The recommended method is to use full text search (mark the field as Analyzed and use the Search() method to query it.");
        }

        private void VisitStartsWith(MethodCallExpression expression)
        {
            GetStringComparisonArgumentsFromExpression(expression, out var firstArg, out var secondArg, out var comparisonArg);

            var memberInfo = GetMember(expression.Object);

            bool exact = _insideExact ? true : ConvertStringComparisonToExact(comparisonArg);

            DocumentQuery.WhereStartsWith(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)), exact);
        }

        private void VisitEndsWith(MethodCallExpression expression)
        {
            GetStringComparisonArgumentsFromExpression(expression, out var firstArg, out var secondArg, out var comparisonArg);

            var memberInfo = GetMember(expression.Object);

            bool exact = _insideExact ? true : ConvertStringComparisonToExact(comparisonArg);

            DocumentQuery.WhereEndsWith(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)), exact);
        }

        private void VisitIsNullOrEmpty(MethodCallExpression expression, bool notEquals)
        {
            var memberInfo = GetMember(expression.Arguments[0]);

            DocumentQuery.OpenSubclause();

            if (notEquals)
                DocumentQuery.WhereNotEquals(memberInfo.Path, null, _insideExact);
            else
                DocumentQuery.WhereEquals(memberInfo.Path, null, _insideExact);

            if (notEquals)
                DocumentQuery.AndAlso();
            else
                DocumentQuery.OrElse();

            if (notEquals)
                DocumentQuery.WhereNotEquals(memberInfo.Path, string.Empty, _insideExact);
            else
                DocumentQuery.WhereEquals(memberInfo.Path, string.Empty, _insideExact);

            DocumentQuery.CloseSubclause();
        }

        private void VisitGreaterThan(BinaryExpression expression)
        {
            bool isStringCompare = false;

            if (IsStringCompareTo(expression))
            {
                if (expression.Left is ConstantExpression)
                {
                    VisitLessThan(Expression.LessThan(expression.Right, expression.Left));
                    return;
                }

                expression = NormalizeStringCompareExpression(expression);

                isStringCompare = true;
            }
            else if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitLessThan(Expression.LessThan(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            var memberType = GetMemberType(memberInfo);

            object value;

            if (isStringCompare)
                value = ((expression.Left as MethodCallExpression).Arguments[0] as ConstantExpression).Value;
            else
                value = GetValueFromExpression(expression.Right, memberType);

            var fieldName = GetFieldNameForRangeQuery(memberInfo, value);

            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.OpenSubclause();
            }

            DocumentQuery.WhereGreaterThan(
                fieldName,
                value,
                _insideExact);

            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.AndAlso();
                DocumentQuery.WhereNotEquals(fieldName, null, _insideExact);
                DocumentQuery.CloseSubclause();
            }
        }

        private void VisitGreaterThanOrEqual(BinaryExpression expression)
        {
            bool isStringCompare = false;

            if (IsStringCompareTo(expression))
            {
                if (expression.Left is ConstantExpression)
                {
                    VisitLessThanOrEqual(Expression.LessThanOrEqual(expression.Right, expression.Left));
                    return;
                }

                expression = NormalizeStringCompareExpression(expression);

                isStringCompare = true;
            }
            else if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitLessThanOrEqual(Expression.LessThanOrEqual(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            var memberType = GetMemberType(memberInfo);

            object value;

            if (isStringCompare)
                value = ((expression.Left as MethodCallExpression).Arguments[0] as ConstantExpression).Value;
            else
                value = GetValueFromExpression(expression.Right, memberType);

            var fieldName = GetFieldNameForRangeQuery(memberInfo, value);

            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.OpenSubclause();

            }
            DocumentQuery.WhereGreaterThanOrEqual(
                fieldName,
                value,
                _insideExact);
            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.AndAlso();
                DocumentQuery.WhereNotEquals(fieldName, null, _insideExact);
                DocumentQuery.CloseSubclause();
            }
        }

        private static bool ShouldExcludeNullTypes(Type memberType)
        {
            return memberType.IsValueType && Nullable.GetUnderlyingType(memberType) != null;
        }

        private void VisitLessThan(BinaryExpression expression)
        {
            bool isStringCompare = false;
            
            if (IsStringCompareTo(expression))
            {
                if (expression.Left is ConstantExpression)
                {
                    VisitGreaterThan(Expression.GreaterThan(expression.Right, expression.Left));
                    return;
                }

                expression = NormalizeStringCompareExpression(expression);

                isStringCompare = true;
            }
            else if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitGreaterThan(Expression.GreaterThan(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            var memberType = GetMemberType(memberInfo);

            object value;

            if (isStringCompare)
                value = ((expression.Left as MethodCallExpression).Arguments[0] as ConstantExpression).Value;
            else
                value = GetValueFromExpression(expression.Right, memberType);

            var fieldName = GetFieldNameForRangeQuery(memberInfo, value);

            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.OpenSubclause();
            }
            DocumentQuery.WhereLessThan(
                fieldName,
                value,
                _insideExact);

            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.AndAlso();
                DocumentQuery.WhereNotEquals(fieldName, null, _insideExact);
                DocumentQuery.CloseSubclause();
            }
        }

        private void VisitLessThanOrEqual(BinaryExpression expression)
        {
            bool isStringCompare = false;

            if (IsStringCompareTo(expression))
            {
                if (expression.Left is ConstantExpression)
                {
                    VisitGreaterThanOrEqual(Expression.GreaterThanOrEqual(expression.Right, expression.Left));
                    return;
                }

                expression = NormalizeStringCompareExpression(expression);

                isStringCompare = true;
            }
            else if(IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitGreaterThanOrEqual(Expression.GreaterThanOrEqual(expression.Right, expression.Left));
                return;
            }
            var memberInfo = GetMember(expression.Left);
            var memberType = GetMemberType(memberInfo);

            object value;

            if (isStringCompare)
                value = ((expression.Left as MethodCallExpression).Arguments[0] as ConstantExpression).Value;
            else
                value = GetValueFromExpression(expression.Right, memberType);

            var fieldName = GetFieldNameForRangeQuery(memberInfo, value);
            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.OpenSubclause();
            }
            DocumentQuery.WhereLessThanOrEqual(
                fieldName,
                value,
                _insideExact);
            if (ShouldExcludeNullTypes(memberType))
            {
                DocumentQuery.AndAlso();
                DocumentQuery.WhereNotEquals(fieldName, null, _insideExact);
                DocumentQuery.CloseSubclause();
            }
        }

        private bool IsStringCompareTo(BinaryExpression expression, bool throwOnInvalidExpression = true)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    break;
                default:
                    return false;
            }

            bool CheckValidComparison(MethodCallExpression methodCall, Expression compareExpression)
            {
                // non-static string compare: x => x.CompareTo("Dave") > 0
                if (methodCall.Object != null)
                {
                    if (IsMemberAccessForQuerySource(methodCall.Object) && !(methodCall.Arguments[0] is ConstantExpression)
                        || !IsMemberAccessForQuerySource(methodCall.Object) && !IsMemberAccessForQuerySource(methodCall.Arguments[0]))
                    {
                        if (throwOnInvalidExpression)
                            throw new NotSupportedException("String comparisons must be between a field and a constant value. " +
                                                            $"`{methodCall.Object}` and `{methodCall.Arguments[0]}` do not satisfy those requirements, so cannot convert {expression} to a proper query.");

                        return false;
                    }
                }

                if (compareExpression is ConstantExpression compareValue && compareValue.Value.Equals(0))
                    return true;

                if (throwOnInvalidExpression)
                    throw new InvalidOperationException("When using string.Compare(), you must always compare against 0");

                return false;
            }

            if (expression.Left is MethodCallExpression leftMethodCall && leftMethodCall.Method.DeclaringType == typeof(string))
            {
                if (leftMethodCall.Method.Name == nameof(string.CompareTo) || leftMethodCall.Method.Name == nameof(string.Compare))
                {
                    if (CheckValidComparison(leftMethodCall, expression.Right) == false)
                        return false;

                    return true;
                }
            }
            else if (expression.Right is MethodCallExpression rightMethodCall && rightMethodCall.Method.DeclaringType == typeof(string))
            {
                if (rightMethodCall.Method.Name == nameof(string.CompareTo) || rightMethodCall.Method.Name == nameof(string.Compare))
                {
                    if (CheckValidComparison(rightMethodCall, expression.Left) == false)
                        return false;

                    return true;
                }
            }

            return false;
        }

        static MethodInfo StringCompareStaticMethodInfo = typeof(string).GetMethod(nameof(string.CompareTo), new Type[] { typeof(string) });


        //x => string.Compare(x.Name, ("Dave")) > 0 -> x => x.Name.CompareTo("Dave") > 0
        private BinaryExpression NormalizeStringCompareExpression(BinaryExpression expression)
        {
            bool leftMethodCall = true;

            var methodCall = expression.Left as MethodCallExpression;

            if (methodCall == null)
            {
                leftMethodCall = false;
                methodCall = expression.Right as MethodCallExpression;
            }

            if (methodCall.Method.Name != nameof(string.Compare))
                return expression;

            if(methodCall.Method.GetParameters().Length != 2)
                throw new NotSupportedException("string.Compare(string, string) is the only supported overload.");

            MemberExpression memberExpression;
            ConstantExpression constantExpression;

            if (IsMemberAccessForQuerySource(methodCall.Arguments[0]))
            {
                memberExpression = methodCall.Arguments[0] as MemberExpression;
                constantExpression = methodCall.Arguments[1] as ConstantExpression;
            }
            else if (IsMemberAccessForQuerySource(methodCall.Arguments[1]))
            {
                memberExpression = methodCall.Arguments[1] as MemberExpression;
                constantExpression = methodCall.Arguments[0] as ConstantExpression;
            }
            else
                throw new NotSupportedException("String comparisons between two constants are not supported. " +
                                                "The comparison should be between a field and a constant value. " +
                                                $"`{methodCall.Arguments[0]}` and `{methodCall.Arguments[1]}` are both constants, so cannot convert {expression} to a proper query.");

            var normalizedExpression = Expression.Call(memberExpression,
                StringCompareStaticMethodInfo,
                constantExpression);

            if (leftMethodCall)
                return Expression.MakeBinary(expression.NodeType, normalizedExpression, expression.Right);

            return Expression.MakeBinary(expression.NodeType, expression.Left, normalizedExpression);
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
                DocumentQuery.WhereExists(memberInfo.Path);
            }
        }

        private void VisitContains(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);
            var containsArgument = expression.Arguments[1];

            DocumentQuery.WhereEquals(new WhereParams
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
                    var whereParams = new WhereParams
                    {
                        FieldName = memberInfo.Path,
                        Value = null,
                        AllowWildcards = false,
                        Exact = _insideExact
                    };

                    if (boolValue)
                        DocumentQuery.WhereNotEquals(whereParams);
                    else
                        DocumentQuery.WhereEquals(whereParams);
                }
                else
                {
                    memberInfo = GetMember(memberExpression);

                    DocumentQuery.WhereEquals(new WhereParams
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

                DocumentQuery.WhereEquals(new WhereParams
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

        private void VisitMethodCall(MethodCallExpression expression)
        {
            if (_conventions.AnyQueryMethodConverters)
            {
                var parameters = new QueryMethodConverter.Parameters<T>(expression, DocumentQuery, VisitExpression);
                if (_conventions.TryConvertQueryMethod(parameters))
                    return;
            }

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

            if (declaringType == typeof(RavenQuery))
            {
                //
                return;
            }

            if (declaringType == typeof(String))
            {
                VisitStringMethodCall(expression);
                return;
            }

            if (declaringType == typeof(Enumerable))
            {
                VisitEnumerableMethodCall(expression);
                return;
            }

            if (declaringType == typeof(Regex))
            {
                VisitRegexMethodCall(expression);
                return;
            }

            if (declaringType.IsGenericType)
            {
                var genericTypeDefinition = declaringType.GetGenericTypeDefinition();
                if (genericTypeDefinition == typeof(ICollection<>) ||
                    genericTypeDefinition == typeof(List<>) ||
                    genericTypeDefinition == typeof(IList<>) ||
                    genericTypeDefinition == typeof(HashSet<>) ||
                    genericTypeDefinition == typeof(Array))
                {
                    VisitListMethodCall(expression);
                    return;
                }
            }

            var method = declaringType.Name + "." + expression.Method.Name;
            throw new NotSupportedException(string.Format("Method not supported: {0}. Expression: {1}.", method, expression));
        }

        private void VisitRegexMethodCall(MethodCallExpression expression)
        {
            if (expression.Method.Name != "IsMatch")
            {
                throw new NotSupportedException(string.Format("Method not supported: Regex.{0}. Expression: {1}.", expression.Method.Name, expression));
            }

            if (expression.Arguments.Count != 2)
            {
                throw new NotSupportedException(string.Format("Regex.IsMatch() overload with {0} arguments is not supported. " +
                                                              "Only Regex.IsMatch(string input, string pattern) overload is supported." +
                                                              "Expression: {1}.", expression.Arguments.Count, expression));
            }

            var memberInfo = GetMember(expression.Arguments[0]);

            DocumentQuery.WhereRegex(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo)).ToString());
        }

        private void VisitLinqExtensionsMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case nameof(LinqExtensions.Search):
                    VisitSearch(expression);
                    break;
                case nameof(LinqExtensions.OrderByScore):
                    DocumentQuery.OrderByScore();
                    VisitExpression(expression.Arguments[0]);
                    break;
                case nameof(LinqExtensions.ThenByScore):
                    VisitExpression(expression.Arguments[0]);
                    DocumentQuery.OrderByScore();
                    break;
                case nameof(LinqExtensions.OrderByScoreDescending):
                    DocumentQuery.OrderByScoreDescending();
                    VisitExpression(expression.Arguments[0]);
                    break;
                case nameof(LinqExtensions.ThenByScoreDescending):
                    VisitExpression(expression.Arguments[0]);
                    DocumentQuery.OrderByScoreDescending();
                    break;
                case nameof(LinqExtensions.Intersect):
                    VisitExpression(expression.Arguments[0]);
                    DocumentQuery.Intersect();
                    _chainedWhere = false;
                    break;
                case nameof(RavenQueryableExtensions.In):
                    var memberInfo = GetMember(expression.Arguments[0]);
                    var objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    DocumentQuery.WhereIn(memberInfo.Path, ((IEnumerable)objects).Cast<object>(), _insideExact);
                    break;
                case nameof(RavenQueryableExtensions.ContainsAny):
                    memberInfo = GetMember(expression.Arguments[0]);
                    objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    DocumentQuery.ContainsAny(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
                    break;
                case nameof(RavenQueryableExtensions.ContainsAll):
                    memberInfo = GetMember(expression.Arguments[0]);
                    objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    DocumentQuery.ContainsAll(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
                    break;
                case nameof(LinqExtensions.Where):
                    VisitQueryableMethodCall(expression);
                    break;
                case nameof(LinqExtensions.Filter):
                    VisitQueryableMethodCall(expression);
                    break;
                case nameof(LinqExtensions.Spatial):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var spatialFieldName);
                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var spatialCriteria);

                    var spatialCriteriaFunc = (Func<SpatialCriteriaFactory, SpatialCriteria>)spatialCriteria;
                    var spatialCriteriaInstance = spatialCriteriaFunc.Invoke(SpatialCriteriaFactory.Instance);

                    if (spatialFieldName is string spatialFieldNameAsString)
                    {
                        DocumentQuery.Spatial(spatialFieldNameAsString, spatialCriteriaInstance);
                    }
                    else if (spatialFieldName is DynamicSpatialField spatialDynamicField)
                    {
                        DocumentQuery.Spatial(spatialDynamicField, spatialCriteriaInstance);
                    }
                    break;
                case nameof(LinqExtensions.OrderByDistance):
                    VisitOrderByDistance(expression.Arguments, descending: false);
                    break;
                case nameof(LinqExtensions.OrderByDistanceDescending):
                    VisitOrderByDistance(expression.Arguments, descending: true);
                    break;
                case nameof(LinqExtensions.Include):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var includeArg);

                    if (includeArg is IncludeBuilder includeBuilder)
                    {
                        DocumentQuery.Include(includeBuilder);
                    }
                    else if (includeArg is string str)
                    {
                        DocumentQuery.Include(str);
                    }
                    break;
                case nameof(LinqExtensions.OrderBy):
                case nameof(LinqExtensions.ThenBy):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var orderByPath);
                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var orderByOrderingTypeOrSorterName);

                    if (orderByOrderingTypeOrSorterName is string orderBySorterName)
                        DocumentQuery.OrderBy((string)orderByPath, orderBySorterName);
                    else
                        DocumentQuery.OrderBy((string)orderByPath, (OrderingType)orderByOrderingTypeOrSorterName);
                    break;
                case nameof(LinqExtensions.OrderByDescending):
                case nameof(LinqExtensions.ThenByDescending):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var orderByDescendingPath);
                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var orderByDescendingOrderingTypeOrSorterName);

                    if (orderByDescendingOrderingTypeOrSorterName is string orderByDescendingSorterName)
                        DocumentQuery.OrderByDescending((string)orderByDescendingPath, orderByDescendingSorterName);
                    else
                        DocumentQuery.OrderByDescending((string)orderByDescendingPath, (OrderingType)orderByDescendingOrderingTypeOrSorterName);

                    break;
                case nameof(LinqExtensions.MoreLikeThis):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments.Last(), out var moreLikeThisAsObject);

                    using (var mlt = DocumentQuery.MoreLikeThis())
                    {
                        var moreLikeThis = (MoreLikeThisBase)moreLikeThisAsObject;

                        mlt.WithOptions(moreLikeThis.Options);

                        if (moreLikeThis is MoreLikeThisUsingDocumentForQuery<T> mltQuery)
                            VisitExpression(mltQuery.ForQuery);
                        else if (moreLikeThis is MoreLikeThisUsingDocument mltDocument)
                            mlt.WithDocument(mltDocument.DocumentJson);
                    }
                    break;
                case nameof(LinqExtensions.AggregateBy):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var aggregateFacet);

                    DocumentQuery.AggregateBy(aggregateFacet as FacetBase);
                    break;
                case nameof(LinqExtensions.AggregateUsing):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var aggregateDocumentKey);

                    DocumentQuery.AggregateUsing(aggregateDocumentKey as string);
                    break;
                case nameof(LinqExtensions.GroupByArrayValues):
                case nameof(LinqExtensions.GroupByArrayContent):
                    EnsureValidDynamicGroupByMethod(expression.Method.Name);

                    VisitExpression(expression.Arguments[0]);

                    var behavior = expression.Method.Name == nameof(LinqExtensions.GroupByArrayValues)
                        ? GroupByArrayBehavior.ByIndividualValues
                        : GroupByArrayBehavior.ByContent;

                    VisitGroupBy(((UnaryExpression)expression.Arguments[1]).Operand, behavior);
                    break;
                case nameof(LinqExtensions.SuggestUsing):
                    VisitExpression(expression.Arguments[0]);

                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var suggestionAsObject);

                    DocumentQuery.SuggestUsing(suggestionAsObject as SuggestionBase);
                    break;
                case nameof(LinqExtensions.Skip):
                    VisitQueryableMethodCall(expression);
                    break;
                case nameof(LinqExtensions.VectorSearch):
                    VisitExpression(expression.Arguments[0]);
                    
                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[3], out var minimumSimilarityObject);

                    if (minimumSimilarityObject is not float minimumSimilarity || minimumSimilarity is <= 0f or > 1.0f)
                        throw new NotSupportedException($"The minimum similarity parameter should be a float in the range [0, 1]. However, it was '{minimumSimilarityObject.GetType().FullName}' with the value '{minimumSimilarityObject.ToString()}'.");
                    
                    LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[1], out var fieldFactoryObject);
                    
                    var fieldBuilder = new VectorEmbeddingFieldFactory<T>();
                    var valueBuilder = new VectorFieldValueFactory();

                    switch (fieldFactoryObject)
                    {
                        case Func<IVectorFieldFactory<T>, IVectorEmbeddingTextField> textFieldFactory:
                        {
                            LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var textFieldValueFactoryObject);
                        
                            textFieldFactory.Invoke(fieldBuilder);

                            if (textFieldValueFactoryObject is not Action<IVectorEmbeddingTextFieldValueFactory> textValueFactory)
                                throw new Exception();
                        
                            textValueFactory.Invoke(valueBuilder);
                            
                            break;
                        }
                        case Func<IVectorFieldFactory<T>, IVectorEmbeddingField> embeddingFieldFactory:
                        {
                            LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var embeddingFieldValueFactoryObject);

                            embeddingFieldFactory.Invoke(fieldBuilder);

                            if (embeddingFieldValueFactoryObject is not Action<IVectorEmbeddingFieldValueFactory> embeddingValueFactory)
                                throw new Exception();

                            embeddingValueFactory.Invoke(valueBuilder);
                            
                            break;
                        }
                        case Func<IVectorFieldFactory<T>, IVectorField> fieldFactory:
                        {
                            LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var embeddingFieldValueFactoryObject);

                            fieldFactory.Invoke(fieldBuilder);

                            if (embeddingFieldValueFactoryObject is not Action<IVectorFieldValueFactory> fieldValueFactory)
                                throw new Exception();

                            fieldValueFactory.Invoke(valueBuilder);
                            
                            break;
                        }
                        
                        default:
                            throw new Exception();
                    }
                    
                    DocumentQuery.VectorSearch(fieldBuilder, valueBuilder, minimumSimilarity);
                    break;
                default:
                    throw new NotSupportedException("Method not supported: " + expression.Method.Name);
            }
        }

        private void VisitOrderByDistance(ReadOnlyCollection<Expression> arguments, bool descending)
        {
            VisitExpression(arguments[0]);

            LinqPathProvider.GetValueFromExpressionWithoutConversion(arguments[1], out var distanceFieldName);
            LinqPathProvider.GetValueFromExpressionWithoutConversion(arguments[2], out var sndArgObj);

            if (sndArgObj is double distanceLatitude) // can only be lat here
            {
                // lat / lng
                LinqPathProvider.GetValueFromExpressionWithoutConversion(arguments[3], out var distanceLongitude);

                if (distanceFieldName is string fieldName)
                {
                    double roundFactor = 0;
                    if (arguments.Count > 4)
                    {
                        LinqPathProvider.GetValueFromExpressionWithoutConversion(arguments[4], out var roundFactorObj);
                        roundFactor = (double)roundFactorObj;
                    }


                    if (descending == false)
                        DocumentQuery.OrderByDistance(fieldName, distanceLatitude, (double)distanceLongitude, roundFactor);
                    else
                        DocumentQuery.OrderByDistanceDescending(fieldName, distanceLatitude, (double)distanceLongitude, roundFactor);
                }
                else if (distanceFieldName is DynamicSpatialField dynamicField)
                {
                    if (descending == false)
                        DocumentQuery.OrderByDistance(dynamicField, (double)distanceLatitude, (double)distanceLongitude);
                    else
                        DocumentQuery.OrderByDistanceDescending(dynamicField, (double)distanceLatitude, (double)distanceLongitude);
                }
                else
                    throw new NotSupportedException("Unknown spatial field name type: " + distanceFieldName.GetType());
            }
            else
            {
                // wkt 
                var distanceShapeWkt = (string)sndArgObj;


                if (distanceFieldName is string fieldName)
                {
                    double roundFactor = 0;
                    if (arguments.Count > 3)
                    {
                        LinqPathProvider.GetValueFromExpressionWithoutConversion(arguments[3], out var roundFactorObj);
                        roundFactor = (double)roundFactorObj;
                    }

                    if (descending == false)
                        DocumentQuery.OrderByDistance(fieldName, distanceShapeWkt, roundFactor);
                    else
                        DocumentQuery.OrderByDistanceDescending(fieldName, distanceShapeWkt, roundFactor);
                }
                else if (distanceFieldName is DynamicSpatialField dynamicField)
                {
                    if (descending == false)
                        DocumentQuery.OrderByDistance(dynamicField, distanceShapeWkt);
                    else
                        DocumentQuery.OrderByDistanceDescending(dynamicField, distanceShapeWkt);
                }
                else
                    throw new NotSupportedException("Unknown spatial field name type: " + distanceFieldName.GetType());
            }
        }

        private void EnsureValidDynamicGroupByMethod(string methodName)
        {
            if (DocumentQuery.CollectionName == null)
                throw new NotSupportedException($"{methodName} method is only supported in dynamic map-reduce queries");
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

            // This has to be set in order to have correct parentheses
            // when using both search and where clause, e.g.
            // where (Category = $p0 or Category = $p1) and search(Name, $p2)
            _insideWhereOrSearchCounter++;
            VisitExpression(target);
            _insideWhereOrSearchCounter--;

            if (expressions.Count > 1)
            {
                DocumentQuery.OpenSubclause();
            }

            foreach (var expression in Enumerable.Reverse(expressions))
            {
                var expressionInfo = GetMember(expression.Arguments[1]);
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract searchTerms value from " + expression);
                }
                var searchTerms = (string)value;
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[3], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract boost value from " + expression);
                }
                var boost = (decimal)value;
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[4], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract options value from " + expression);
                }
                var options = (SearchOptions)value;

                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[5], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract operator from " + expression);
                }

                var @operator = (SearchOperator)value;

                if (_chainedWhere && options.HasFlag(SearchOptions.And))
                {
                    DocumentQuery.AndAlso();
                }
                
                if (options.HasFlag(SearchOptions.Not))
                {
                    if (options.HasFlag(SearchOptions.And) && IsPreviousSearchOnSameField(target, expression))
                    {
                        DocumentQuery.NegateNext();
                        DocumentQuery.Search(expressionInfo.Path, searchTerms, @operator);
                    }

                    else
                        WhereExistsAndNegatedSearch(expressionInfo, searchTerms, @operator);
                }
                else
                {
                    DocumentQuery.Search(expressionInfo.Path, searchTerms, @operator);
                }

                DocumentQuery.Boost(boost);

                if (options.HasFlag(SearchOptions.And))
                {
                    _chainedWhere = true;
                }
            }

            if (expressions.Count > 1)
            {
                DocumentQuery.CloseSubclause();
            }

            if (LinqPathProvider.GetValueFromExpressionWithoutConversion(searchExpression.Arguments[4], out value) == false)
            {
                throw new InvalidOperationException("Could not extract value from " + searchExpression);
            }

            if (((SearchOptions)value).HasFlag(SearchOptions.Guess))
                _chainedWhere = true;
            
            return;

            void WhereExistsAndNegatedSearch(ExpressionInfo expressionInfo, string searchTerms, SearchOperator @operator)
            {
                DocumentQuery.OpenSubclause();
                DocumentQuery.WhereExists(expressionInfo.Path);
                DocumentQuery.AndAlso();
                DocumentQuery.NegateNext();
                DocumentQuery.Search(expressionInfo.Path, searchTerms, @operator);
                DocumentQuery.CloseSubclause();
            }
            
            bool IsPreviousSearchOnSameField(Expression searchTargetExpression, Expression currentSearchExpression)
            {
                if (searchTargetExpression is MethodCallExpression targetMce && currentSearchExpression is MethodCallExpression currentSearchMce)
                {
                    var targetExpressionInfo = GetMember(targetMce.Arguments[1]);
                    var currentSearchExpressionInfo = GetMember(currentSearchMce.Arguments[1]);
                
                    return targetExpressionInfo.Path == currentSearchExpressionInfo.Path;
                }
            
                return false;
            }
        }

        private void VisitListMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "Contains":
                    {
                        var memberInfo = GetMember(expression.Object);

                        var containsArgument = expression.Arguments[0];

                        DocumentQuery.WhereEquals(new WhereParams
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

        private void VisitEnumerableMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "Any":
                    {
                        if (expression.Arguments.Count == 1 && expression.Arguments[0].Type == typeof(string))
                        {
                            VisitIsNullOrEmpty(expression, notEquals: true);
                        }
                        else
                        {
                            using (AnyModeScope())
                            {
                                VisitAny(expression);
                            }
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
                        VisitIsNullOrEmpty(expression, notEquals: false);
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
                    if (expression.Arguments[0].Type.IsGenericType)
                    {
                        var type = expression.Arguments[0].Type.GetGenericArguments()[0];
                        DocumentQuery.AddRootType(type);
                    }

                    //set the _ofType variable only if OfType call precedes the projection
                    if (_isSelectArg && expression.Type.IsGenericType)
                    {
                        _ofType = expression.Type.GetGenericArguments()[0];
                    }

                    VisitExpression(expression.Arguments[0]);
                    break;
                case "Filter":
                {
                    _insideFilterCounter++;
                    VisitExpression(expression.Arguments[0]);
                    using (FilterModeScope(true))
                    {
                        if (_chainedWhere)
                        {
                            DocumentQuery.AndAlso();
                            DocumentQuery.OpenSubclause();
                        }

                        if (_chainedWhere == false && _insideFilterCounter > 1)
                            DocumentQuery.OpenSubclause();

                        VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        if (_chainedWhere == false && _insideFilterCounter > 1)
                            DocumentQuery.CloseSubclause();
                        if (_chainedWhere)
                            DocumentQuery.CloseSubclause();
                        _insideFilterCounter--;
                        if (expression.Arguments.Count is 3 && LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out var limit))
                        {
                            AddFilterLimit((int)limit);
                        }
                    }

                    break;
                }
                case "Where":
                    {
                        using (FilterModeScope(@on: false))
                        {
                            _insideWhereOrSearchCounter++;
                            VisitExpression(expression.Arguments[0]);
                            if (_chainedWhere)
                            {
                                DocumentQuery.AndAlso();
                                DocumentQuery.OpenSubclause();
                            }
                            if (_chainedWhere == false && _insideWhereOrSearchCounter > 1)
                                DocumentQuery.OpenSubclause();

                            if (expression.Arguments.Count == 3)
                                _insideExact = (bool)GetValueFromExpression(expression.Arguments[2], typeof(bool));

                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                            _insideExact = false;

                            if (_chainedWhere == false && _insideWhereOrSearchCounter > 1)
                                DocumentQuery.CloseSubclause();
                            if (_chainedWhere)
                                DocumentQuery.CloseSubclause();
                            _chainedWhere = true;
                            _insideWhereOrSearchCounter--;
                        }
                        break;
                    }
                case "Select":
                    {
                        if (expression.Arguments[0].Type.IsGenericType &&
                            (expression.Arguments[0].Type.GetGenericTypeDefinition() == typeof(IQueryable<>) ||
                            expression.Arguments[0].Type.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>) ||
                            expression.Arguments[0].Type.GetGenericTypeDefinition() == typeof(IRavenQueryable<>)) &&
                            expression.Arguments[0].Type != expression.Arguments[1].Type)
                        {
                            DocumentQuery.AddRootType(expression.Arguments[0].Type.GetGenericArguments()[0]);
                        }

                        if (ExpressionHasNestedLambda(expression, out var lambdaExpression))
                        {
                            CheckForLetOrLoadFromSelect(expression, lambdaExpression);
                        }

                        _isSelectArg = true;
                        VisitExpression(expression.Arguments[0]);

                        _isSelectArg = false;

                        var operand = ((UnaryExpression)expression.Arguments[1]).Operand;

                        if (DocumentQuery.IsDynamicMapReduce)
                        {
                            VisitSelectAfterGroupBy(operand, _groupByElementSelector);
                            _groupByElementSelector = null;
                        }
                        else if (_selectLoad)
                        {
                            AddFromAlias(lambdaExpression.Parameters[0].Name);
                            _selectLoad = false;
                        }

                        else
                        {
                            var initialSelect = _insideSelect;
                            var initialCount = FieldsToFetch.Count;
                            
                            _insideSelect++;
                            VisitSelect(operand);
                            _insideSelect--;
                            
                            if (initialSelect == 0 && initialCount > 0 && FieldsToFetch.Count != initialCount)
                                QueryData.ThrowProjectionIsAlreadyDone();
                        }
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
                                DocumentQuery.AndAlso();
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
                                DocumentQuery.AndAlso();

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
                        if (expression.Arguments.Count == 2 && expression.Arguments[0] is not ConstantExpression)
                            _subClauseDepth++;
                        
                        VisitExpression(expression.Arguments[0]);
                        if (expression.Arguments.Count == 2)
                        {
                            if (_chainedWhere)
                                DocumentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                            
                            if (_subClauseDepth > 0)
                                _subClauseDepth--;
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
                                DocumentQuery.AndAlso();
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
                                DocumentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                        }

                        VisitLongCount();
                        break;
                    }
                case "Distinct":
                    if (expression.Arguments.Count == 1)
                    {
                        DocumentQuery.Distinct();
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
                    EnsureValidDynamicGroupByMethod("GroupBy");

                    if (expression.Arguments.Count == 5) // GroupBy(x => keySelector, x => elementSelector, x => resultSelector, IEqualityComparer)
                        throw new NotSupportedException("Dynamic map-reduce queries does not support a custom equality comparer");

                    VisitExpression(expression.Arguments[0]);
                    VisitGroupBy(((UnaryExpression)expression.Arguments[1]).Operand, GroupByArrayBehavior.NotApplicable);

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

        private bool IsFilterModeActive() => DocumentQuery switch
        {
            DocumentQuery<T> documentQuery => documentQuery.IsFilterActive,
            AsyncDocumentQuery<T> asyncDocumentQuery => asyncDocumentQuery.IsFilterActive,
            _ => throw new NotSupportedException($"Currently {DocumentQuery.GetType()} doesn't support {nameof(LinqExtensions.Filter)}.") 
        };
        

        private IDisposable FilterModeScope(bool @on) => DocumentQuery switch
        {
            DocumentQuery<T> documentQuery => documentQuery.SetFilterMode(@on),
            AsyncDocumentQuery<T> asyncDocumentQuery => asyncDocumentQuery.SetFilterMode(@on),
            _ => throw new NotSupportedException($"Currently {DocumentQuery.GetType()} doesn't support {nameof(LinqExtensions.Filter)}.") 
        };

        private IDisposable AnyModeScope() => DocumentQuery switch
        {
            DocumentQuery<T> documentQuery => documentQuery.SetAnyMode(),
            AsyncDocumentQuery<T> asyncDocumentQuery => asyncDocumentQuery.SetAnyMode(),
            _ => throw new NotSupportedException($"Currently {DocumentQuery.GetType()} doesn't support {nameof(Enumerable.Any)} method.")
        };
        
        private void AddFilterLimit(int filterLimit)
        {
            switch (DocumentQuery)
            {
                case DocumentQuery<T> documentQuery:
                    documentQuery.AddFilterLimit(filterLimit);
                    break;
                case AsyncDocumentQuery<T> asyncDocumentQuery:
                    asyncDocumentQuery.AddFilterLimit(filterLimit);
                    break;
                default:
                    throw new NotSupportedException($"Currently {DocumentQuery.GetType()} doesn't support {nameof(LinqExtensions.Filter)}.");
            }
        }

        private void CheckForLetOrLoadFromSelect(MethodCallExpression expression, LambdaExpression lambdaExpression)
        {
            var parameterName = lambdaExpression.Parameters[0].Name;

            if (parameterName.StartsWith(JavascriptConversionExtensions.TransparentIdentifier))
            {
                _insideLet++;
                return;
            }

            if (_loadAlias != null)
            {
                HandleLoadFromSelectIfNeeded(lambdaExpression);
                _loadAlias = null;
                return;
            }

            LambdaExpression innerLambda = null;
            if (expression.Arguments[0] is MethodCallExpression mce &&
                mce.Method.Name == "Select" &&
                ExpressionHasNestedLambda(mce, out innerLambda) &&
                innerLambda.Body is MethodCallExpression innerMce &&
                CheckForLoad(innerMce))
            {
                _loadAlias = parameterName;
                return;
            }

            if (HandleLoadFromSelectIfNeeded(lambdaExpression) == false && innerLambda?.Body is NewExpression)
            {
                // handle select after select new
                // treat it as 'let' and use js output function

                if (_manualLet.IsSet)
                {
                    throw new NotSupportedException("Unsupported query expression. Try to use 'Let' instead of multiple Selects");
                }

                _insideLet++;
                _manualLet.IsSet = true;
                _manualLet.Name = parameterName;
                AddFromAlias(innerLambda.Parameters[0].Name);
            }
        }

        private bool CheckForLoad(MethodCallExpression mce)
        {
            return mce.Method.Name == nameof(RavenQuery.Load) &&
                   (mce.Method.DeclaringType == typeof(RavenQuery) ||
                    mce.Object?.Type == typeof(IDocumentSession));
        }
        
        private bool CheckForNestedLoad(LambdaExpression lambdaExpression, out MethodCallExpression mce, out string selectPath)
        {
            if (lambdaExpression.Body is MemberExpression memberExpression)
            {
                if (JavascriptConversionExtensions.GetInnermostExpression(memberExpression, out string path, out _)
                        is MethodCallExpression methodCallExpression &&
                    CheckForLoad(methodCallExpression))
                {
                    selectPath = path == string.Empty
                        ? memberExpression.Member.Name
                        : $"{path}.{memberExpression.Member.Name}";
                    mce = methodCallExpression;
                    return true;
                }
            }

            selectPath = null;
            mce = null;
            return false;

        }
        
        private bool HandleLoadFromSelectIfNeeded(LambdaExpression lambdaExpression)
        {
            if (CheckForNestedLoad(lambdaExpression, out var mce, out var member) == false)
            {
                if (!(lambdaExpression.Body is MethodCallExpression methodCall) ||
                    CheckForLoad(methodCall) == false)
                    return false;

                mce = methodCall;
            }

            //add load token

            if (_loadTokens == null)
            {
                _loadTokens = new List<LoadToken>();
                _loadAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            string arg;
            if (mce.Arguments[0] is ConstantExpression constantExpression &&
                constantExpression.Type == typeof(string))
            {
                arg = DocumentQuery.ProjectionParameter(constantExpression.Value);
            }
            else
            {
                arg = ToJs(mce.Arguments[0]);
            }

            AddLoadToken(arg, _loadAlias ?? DefaultLoadAlias);

            if (_loadAlias == null)
            {
                var path = DefaultLoadAlias;
                if (member != null)
                {
                    path = $"{path}.{member}";
                }

                AddToFieldsToFetch(path, path);
            }

            _selectLoad = true;
            return true;
        }

        private static bool ExpressionHasNestedLambda(MethodCallExpression expression, out LambdaExpression lambdaExpression)
        {
            if (expression.Arguments.Count > 1 &&
                expression.Arguments[1] is UnaryExpression unaryExpression &&
                unaryExpression.Operand is LambdaExpression lambda &&
                lambda.Parameters[0] != null)
            {
                lambdaExpression = lambda;
                return true;
            }

            lambdaExpression = null;
            return false;
        }

        private void VisitGroupBy(Expression expression, GroupByArrayBehavior arrayBehavior)
        {
            var lambdaExpression = expression as LambdaExpression;

            if (lambdaExpression == null)
                throw new NotSupportedException("We expect GroupBy statement to have lambda expression");

            var body = lambdaExpression.Body;
            switch (body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var groupByExpression = LinqPathProvider.GetMemberExpression(lambdaExpression);

                    var singleGroupByFieldName = GetSelectPath(groupByExpression);

                    var groupByFieldType = groupByExpression.Type;

                    if (IsCollection(groupByFieldType))
                    {
                        switch (arrayBehavior)
                        {
                            case GroupByArrayBehavior.NotApplicable:
                                throw new InvalidOperationException(
                                    "Please use one of dedicated methods to group by collection: " +
                                    $"{nameof(LinqExtensions.GroupByArrayValues)}, {nameof(LinqExtensions.GroupByArrayContent)}. " +
                                    $"Field name: {singleGroupByFieldName}");
                            case GroupByArrayBehavior.ByIndividualValues:
                                singleGroupByFieldName += "[]";
                                break;
                            case GroupByArrayBehavior.ByContent:
                                break;
                        }
                    }

                    DocumentQuery.GroupBy(singleGroupByFieldName);
                    break;
                case ExpressionType.New:
                    var newExpression = ((NewExpression)body);

                    AddCompositeGroupByKey(newExpression);
                    break;
                case ExpressionType.MemberInit:
                    var memberInitExpression = ((MemberInitExpression)body);

                    for (int index = 0; index < memberInitExpression.Bindings.Count; index++)
                    {
                        var field = memberInitExpression.Bindings[index] as MemberAssignment;

                        if (field == null)
                            throw new InvalidOperationException($"We expected MemberAssignment expression while got {memberInitExpression.Bindings[index].GetType().FullName} in GroupBy");

                        var originalField = GetSelectPath((MemberExpression)field.Expression);

                        DocumentQuery.GroupBy(originalField);

                        AddGroupByAliasIfNeeded(field.Member, originalField);
                    }
                    break;
                case ExpressionType.Constant:
                    var constantExpression = (ConstantExpression)body;

                    string constantValue;

                    switch (constantExpression.Value)
                    {
                        case string stringConst:
                            constantValue = $"\"{stringConst}\"";
                            break;
                        default:
                            constantValue = constantExpression.Value?.ToString() ?? "null";
                            break;
                    }

                    DocumentQuery.GroupBy(constantValue);
                    break;
                case ExpressionType.ArrayLength:
                    var unaryExpression = (UnaryExpression)lambdaExpression.Body;

                    switch (unaryExpression.NodeType)
                    {
                        case ExpressionType.ArrayLength:
                            var prop = GetMember(unaryExpression.Operand);

                            DocumentQuery.GroupBy(prop.Path + ".Length");
                            break;
                        default:
                            throw new NotSupportedException($"Unhandled expression type in group by: {unaryExpression.NodeType}");
                    }
                    break;
                case ExpressionType.Call:
                    // .GroupByArrayValues(x => x.Lines.Select(y => y.Product))
                    // .GroupByArrayContent(x => x.Lines.Select(y => y.Product))

                    var method = (MethodCallExpression)lambdaExpression.Body;
                    var methodName = method.Method.Name;

                    if (methodName != "Select")
                    {
                        throw new NotSupportedException(
                            $"Expression passed to {nameof(LinqExtensions.GroupByArrayValues)} or {nameof(LinqExtensions.GroupByArrayContent)} " +
                            $"can only contain Select method while it got {methodName}");
                    }

                    var parts = new List<string>();

                    foreach (var methodArgument in method.Arguments)
                    {
                        switch (methodArgument.NodeType)
                        {
                            case ExpressionType.MemberAccess:
                                AddPart();
                                break;
                            case ExpressionType.Lambda:
                                var lambda = (LambdaExpression)methodArgument;

                                switch (lambda.Body.NodeType)
                                {
                                    case ExpressionType.MemberAccess:
                                        AddPart();
                                        break;
                                    case ExpressionType.New:
                                        AddCompositeGroupByKey((NewExpression)lambda.Body, parts);
                                        return;
                                    default:
                                        throw new NotSupportedException($"Unsupported body node type of lambda expression: {lambda.Body.NodeType}");
                                }
                                break;
                            default:
                                throw new NotSupportedException($"Unsupported argument type of Select call: {methodArgument.NodeType}");
                        }

                        void AddPart()
                        {
                            var field = LinqPathProvider.GetMemberExpression(methodArgument);

                            var path = GetSelectPath(field);

                            if (IsCollection(field.Type))
                                path += "[]";

                            parts.Add(path);
                        }
                    }

                    DocumentQuery.GroupBy((string.Join(".", parts), arrayBehavior == GroupByArrayBehavior.ByContent ? GroupByMethod.Array : GroupByMethod.None));
                    break;
                default:
                    throw new NotSupportedException("Node not supported in GroupBy: " + body.NodeType);
            }
        }

        private void AddCompositeGroupByKey(NewExpression newExpression, List<string> prefix = null)
        {
            for (int index = 0; index < newExpression.Arguments.Count; index++)
            {
                if (!(newExpression.Arguments[index] is MemberExpression memberExpression))
                {
                    throw new InvalidOperationException($"Illegal expression of type {newExpression.Arguments[index].GetType()} " +
                                                        $"in GroupBy clause : {newExpression.Arguments[index]}." + Environment.NewLine +
                                                        "You cannot do computation in group by dynamic query, " +
                                                        "you can group on properties / fields only.");
                }

                var originalField = GetSelectPath(memberExpression);

                if (prefix != null)
                    originalField = string.Join(".", prefix.Union(new[] { originalField }));

                DocumentQuery.GroupBy(originalField);

                AddGroupByAliasIfNeeded(newExpression.Members[index], originalField);
            }
        }
        
        private void AddGroupByAliasIfNeeded(MemberInfo aliasMember, string originalField)
        {
            var alias = GetSelectPath(aliasMember);

            if (alias != null && originalField.Equals(alias, StringComparison.Ordinal) == false)
            {
                if (DocumentQuery is DocumentQuery<T> docQuery)
                    docQuery.AddGroupByAlias(originalField, alias);
                else if (DocumentQuery is AsyncDocumentQuery<T> asyncDocQuery)
                    asyncDocQuery.AddGroupByAlias(originalField, alias);
            }
        }

        private void VisitOrderBy(LambdaExpression expression, bool descending)
        {
            var result = GetMemberDirect(expression.Body);

            var fieldType = result.Type;
            var fieldName = result.Path;
            if (_isMapReduce == false &&
                result.MaybeProperty != null &&
                QueryGenerator.Conventions.FindIdentityProperty(result.MaybeProperty))
            {
                fieldName = Constants.Documents.Indexing.Fields.DocumentIdFieldName;
                fieldType = typeof(string);
            }

            var rangeType = QueryGenerator.Conventions.GetRangeType(fieldType);

            var ordering = OrderingUtil.GetOrderingFromRangeType(rangeType);
            if (descending)
                DocumentQuery.OrderByDescending(fieldName, ordering);
            else
                DocumentQuery.OrderBy(fieldName, ordering);
        }

        private int _insideSelect;
        private readonly bool _isMapReduce;
        private readonly Type _originalQueryType;
        private readonly DocumentConventions _conventions;
        private readonly bool _isProjectInto;

        private void VisitSelect(Expression operand)
        {
            if (FieldsToFetch is {Count: > 0})
                QueryData.ThrowProjectionIsAlreadyDone();

            var lambdaExpression = operand as LambdaExpression;
            var body = lambdaExpression != null ? lambdaExpression.Body : operand;
            switch (body.NodeType)
            {
                case ExpressionType.Convert:
                    _insideSelect++;
                    try
                    {
                        VisitSelect(((UnaryExpression)body).Operand);
                    }
                    finally
                    {
                        _insideSelect--;
                    }
                    break;
                case ExpressionType.MemberAccess:
                    SelectMemberAccess(body);
                    break;
                //Anonymous types come through here .Select(x => new { x.Cost } ) doesn't use a member initializer, even though it looks like it does
                //See http://blogs.msdn.com/b/sreekarc/archive/2007/04/03/immutable-the-new-anonymous-type.aspx
                case ExpressionType.New:
                    SelectNewExpression((NewExpression)body, lambdaExpression);
                    break;
                //for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
                case ExpressionType.MemberInit:
                    SelectMemberInit((MemberInitExpression)body, lambdaExpression);
                    break;
                case ExpressionType.Parameter: // want the full thing, so just pass it on.
                    break;
                //for example .Select(product => product.Properties["VendorName"])
                case ExpressionType.Call:
                    SelectCall(body, lambdaExpression);
                    break;

                default:
                    throw new NotSupportedException("Node not supported: " + body.NodeType);
            }
        }
        
        private void SelectCall(Expression body, LambdaExpression lambdaExpression)
        {
            FieldsToFetch?.Clear();

            if (body is MethodCallExpression call)
            {
                if (LinqPathProvider.IsTimeSeriesCall(call))
                {
                    HandleSelectTimeSeries(call);
                    return;
                }

                if (LinqPathProvider.IsCompareExchangeCall(call))
                {
                    AddToFieldsToFetch(ToJs(call), "cmpxchg");
                    return;
                }

                if (IsRawCall(call))
                {
                    HandleSelectRaw(call, lambdaExpression);
                    return;
                }
            }

            var expressionInfo = GetMember(body);
            var path = expressionInfo.Path;
            string alias = null;

            if (expressionInfo.Args != null)
            {
                AddCallArgumentsToPath(expressionInfo.Args, lambdaExpression?.Parameters[0].Name, ref path, out alias);
            }

            AddToFieldsToFetch(path, alias);
        }

        private void SelectMemberInit(MemberInitExpression memberInitExpression, LambdaExpression lambdaExpression)
        {
            if (memberInitExpression.NewExpression.Arguments.Any())
                throw new NotSupportedException($"Using constructor with parameters in projection is not supported. {memberInitExpression}");
            
            _newExpressionType = memberInitExpression.NewExpression.Type;
            FieldsToFetch?.Clear();

            if (_declareBuilder != null)
            {
                AddReturnStatementToOutputFunction(memberInitExpression);
                return;
            }
            
            if (FromAlias != null)
            {
                //lambda 2 js
                _jsSelectBody = TranslateSelectBodyToJs(memberInitExpression);
                return;
            }

            foreach (MemberBinding t in memberInitExpression.Bindings)
            {
                var field = t as MemberAssignment;
                if (field == null)
                    continue;

                if (field.Expression is ConstantExpression constant)
                {
                    AddToFieldsToFetch(GetConstantValueAsString(constant.Value), GetSelectPath(field.Member));
                    continue;
                }

                if (field.Expression.NodeType == ExpressionType.ArrayLength)
                {
                    var info = GetMember(field.Expression);
                    AddToFieldsToFetch(info.Path, GetSelectPath(field.Member));
                    continue;
                }

                if (TryGetMemberExpression(field.Expression, out var member) && HasComputation(member) == false)
                {
                    var renamedField = GetSelectPathOrConstantValue(member);
                    AddToFieldsToFetch(renamedField, GetSelectPath(field.Member));
                    continue;
                }

                if (field.Expression is MethodCallExpression mce)
                {
                    if (LinqPathProvider.IsCounterCall(mce))
                    {
                        HandleSelectCounter(mce, lambdaExpression, field.Member);
                        continue;
                    }

                    if (LinqPathProvider.IsTimeSeriesCall(mce))
                    {
                        HandleSelectTimeSeries(mce, GetSelectPath(field.Member));
                        continue;
                    }
                }

                //lambda 2 js
                if (FromAlias == null)
                {
                    AddFromAlias(lambdaExpression?.Parameters[0].Name);
                }

                FieldsToFetch?.Clear();
                _jsSelectBody = TranslateSelectBodyToJs(memberInitExpression);

                break;
            }
        }

        private void SelectNewExpression(NewExpression newExpression, LambdaExpression lambdaExpression)
        {
            if (_insideLet > 0)
            {
                VisitLet(newExpression);
                _insideLet--;
                return;
            }

            _newExpressionType = newExpression.Type;
            FieldsToFetch?.Clear();

            if (_declareBuilder != null)
            {
                AddReturnStatementToOutputFunction(newExpression);
                return;
            }

            if (FromAlias != null)
            {
                //lambda 2 js
                _jsSelectBody = TranslateSelectBodyToJs(newExpression);
                return;
            }

            if (newExpression.Members == null)
                //SelectNewExpression is used only for anonymous types and in this situation there are Members as well
                throw new NotSupportedException($"Using constructor with parameters in projection is not supported. {newExpression}");
                
            for (int index = 0; index < newExpression.Arguments.Count; index++)
            {
                if (newExpression.Arguments[index] is ConstantExpression constant)
                {
                    AddToFieldsToFetch(GetConstantValueAsString(constant.Value), GetSelectPath(newExpression.Members[index]));
                    continue;
                }

                if (newExpression.Arguments[index].NodeType == ExpressionType.ArrayLength)
                {
                    var info = GetMember(newExpression.Arguments[index]);
                    AddToFieldsToFetch(info.Path, GetSelectPath(newExpression.Members[index]));
                    continue;
                }

                if (newExpression.Arguments[index] is MemberExpression member && HasComputation(member) == false)
                {
                    AddToFieldsToFetch(GetSelectPathOrConstantValue(member), GetSelectPath(newExpression.Members[index]));
                    continue;
                }

                if (newExpression.Arguments[index] is MethodCallExpression mce)
                {
                    if (LinqPathProvider.IsCounterCall(mce))
                    {
                        HandleSelectCounter(mce, lambdaExpression, newExpression.Members[index]);
                        continue;
                    }

                    if (LinqPathProvider.IsTimeSeriesCall(mce))
                    {
                        HandleSelectTimeSeries(mce, GetSelectPath(newExpression.Members[index]));
                        continue;
                    }
                }

                //lambda 2 js
                if (FromAlias == null)
                {
                    AddFromAlias(lambdaExpression?.Parameters[0].Name);
                }

                FieldsToFetch?.Clear();
                _jsSelectBody = TranslateSelectBodyToJs(newExpression);
                break;
            }
        }

        private void SelectMemberAccess(Expression body)
        {
            var memberExpression = ((MemberExpression)body);

            var selectPath = GetSelectPath(memberExpression);
            FieldsToFetch?.Clear(); // this overwrite any previous projection
            AddToFieldsToFetch(selectPath, selectPath);

            if (_insideSelect == 1 && FieldsToFetch != null)
            {
                foreach (var fieldToFetch in FieldsToFetch)
                {
                    if (fieldToFetch.Name != memberExpression.Member.Name)
                        continue;

                    fieldToFetch.Alias = null;
                }
            }

            if (_declareBuilder != null)
                AddReturnStatementToOutputFunction(body);
        }

        private void AddFromAliasOrRenameArgument(ref string arg)
        {
            if (FromAlias == null)
            {
                AddFromAlias(arg);
            }
            else
            {
                arg = FromAlias;
            }
        }

        private void HandleSelectCounter(MethodCallExpression methodCallExpression, LambdaExpression lambdaExpression, MemberInfo member)
        {
            var counterInfo = LinqPathProvider.CreateCounterResult(methodCallExpression);

            AddCallArgumentsToPath(counterInfo.Args, lambdaExpression?.Parameters[0].Name, ref counterInfo.Path, out _);
            AddToFieldsToFetch(counterInfo.Path, GetSelectPath(member));
        }

        private void AddCallArgumentsToPath(string[] mceArgs, string parameter, ref string path, out string alias)
        {
            alias = null;

            if (mceArgs[0] == parameter)
            {
                AddFromAliasOrRenameArgument(ref mceArgs[0]);
            }

            var args = mceArgs[0];
            if (mceArgs.Length == 2)
            {
                args += $", {QueryFieldUtil.EscapeIfNecessary(mceArgs[1])}";
            }

            if (path == LinqPathProvider.CounterMethodName)
            {
                alias = mceArgs[mceArgs.Length - 1];
            }

            path = $"{path}({args})";
        }

        private void HandleSelectTimeSeries(MethodCallExpression callExpression, string alias = null)
        {
            var visitor = new TimeSeriesQueryVisitor<T>(this);
            var tsQueryText = visitor.Visit(callExpression);
            string path = $"{Constants.TimeSeries.SelectFieldName}({tsQueryText})";
            alias ??= Constants.TimeSeries.QueryFunction + FieldsToFetch.Count;

            AddToFieldsToFetch(path, alias);
        }

        private void HandleSelectRaw(MethodCallExpression callExpression, LambdaExpression lambdaExpression)
        {
            AddFromAlias(lambdaExpression.Parameters[0].Name);
            _declareBuilder ??= new StringBuilder();
            AddReturnStatementToOutputFunction(callExpression);
        }

        private string GetSelectPathOrConstantValue(MemberExpression member)
        {
            if (member.Member is FieldInfo filedInfo && member.Expression is ConstantExpression constantExpression)
            {
                var value = filedInfo.GetValue(constantExpression.Value);
                return GetConstantValueAsString(value);
            }

            return GetSelectPath(member);
        }

        private static string GetConstantValueAsString(object value)
        {
            if (value is string || value is char)
            {
                return $"\"{value}\"";
            }

            return value?.ToString();
        }

        private void AddReturnStatementToOutputFunction(Expression expression)
        {
            if (_manualLet.IsSet)
            {
                _typedParameterSupport = new JavascriptConversionExtensions.TypedParameterSupport(_manualLet.Name);
            }
            var js = TranslateSelectBodyToJs(expression);
            _declareBuilder.Append('\t').Append("return ").Append(js).Append(';');

            var paramBuilder = new StringBuilder();
            paramBuilder.Append(FromAlias);

            if (_loadTokens != null)
            {
                //need to add loaded documents 
                for (var i = 0; i < _loadTokens.Count; i++)
                {
                    var alias = _loadTokens[i].Alias.EndsWith("[]")
                        ? _loadTokens[i].Alias.Substring(0, _loadTokens[i].Alias.Length - 2)
                        : _loadTokens[i].Alias;

                    paramBuilder.Append(", ").Append(alias);
                }
            }

            if (_projectionParameters != null)
            {
                for (var i = 0; i < _projectionParameters.Count; i++)
                {
                    paramBuilder.Append(", ").Append(_projectionParameters[i]);
                }
            }

            _declareTokens ??= new List<DeclareToken>();
            var declareToken = DeclareToken.CreateFunction("output", _declareBuilder.ToString(), paramBuilder.ToString());
            _declareTokens.Add(declareToken);
            _jsSelectBody = $"output({declareToken.Parameters})";
        }

        private void VisitLet(NewExpression expression)
        {
            if (_insideWhereOrSearchCounter > 0)
                throw new NotSupportedException("Queries with a LET clause before a WHERE clause are not supported. " +
                                                "WHERE clauses should appear before any LET clauses.");

            if (FromAlias == null)
            {
                var parameter = expression.Arguments[0] as ParameterExpression;
                AddFromAlias(parameter?.Name);
            }

            if (_projectionParameters == null)
            {
                _projectionParameters = new List<string>();
            }

            TranslateLetBody(expression);
        }

        private void TranslateLetBody(NewExpression expression)
        {
            StringBuilder wrapper = null;
            int start = 1, upTo = 2;
            if (_manualLet.IsSet)
            {
                // treat this 'Select(x => new {...})' expression as 'let'
                // wrap all the projection fields inside an object and append it to output function, e.g. :
                // 'Select(x => new { Foo = x.Foo, Bar = x.Bar }).Select(y => ...)' turns into :
                // function output(x){
                //  var y = { Foo : x.Foo, Bar : x.Bar }
                //  return {...}
                // }

                start = 0;
                upTo = expression.Arguments.Count;
                wrapper = new StringBuilder();
                wrapper.Append('{')
                    .Append(Environment.NewLine);
            }

            for (var i = start; i < upTo; i++)
            {
                var arg = expression.Arguments[i];
                var memberInfo = expression.Members[i];
                var name = GetSelectPath(memberInfo);

                if (IsRawOrTimeSeriesCall(arg, out string script))
                {
                    AppendLineToOutputFunction(name, script, wrapper);
                    continue;
                }

                // if _declareBuilder != null then we already have an 'output' function
                // the load-argument might depend on previous statements inside the function body
                // use js load() method instead of a LoadToken
                var shouldUseLoadToken = _declareBuilder == null &&
                                         arg.NodeType != ExpressionType.Conditional &&
                                         (!(arg is MethodCallExpression methodCall) || methodCall.Method.Name == nameof(RavenQuery.Load));

                var loadSupport = new JavascriptConversionExtensions.LoadSupport(_loadTypes) { DoNotTranslate = shouldUseLoadToken };
                var js = ToJs(arg, false, loadSupport);

                if (loadSupport.HasLoad && shouldUseLoadToken)
                {
                    HandleLoad(arg, loadSupport, name, js, wrapper);
                    continue;
                }

                AppendLineToOutputFunction(name, js, wrapper);
            }

            if (wrapper != null)
            {
                wrapper.Append('}').Append(Environment.NewLine);
                AppendLineToOutputFunction(_manualLet.Name, wrapper.ToString());
            }
        }


        private void HandleLoad(Expression expression, JavascriptConversionExtensions.LoadSupport loadSupport, string name, string js, StringBuilder wrapper)
        {
            string arg;
            if (loadSupport.Arg is ConstantExpression constantExpression && constantExpression.Type == typeof(string))
            {
                arg = DocumentQuery.ProjectionParameter(constantExpression.Value);
            }
            else if (loadSupport.Arg is MemberExpression memberExpression && IsDictionaryOnDictionaryExpression(memberExpression) == false)
            {
                // if memberExpression is <>h__TransparentIdentifierN...TransparentIdentifier1.TransparentIdentifier0.something
                // then the load-argument is 'something' which is not a real path (a real path should be 'x.something')
                // but a name of a variable that was previously defined in a 'let' statement.

                var param = memberExpression.Expression is ParameterExpression parameter
                    ? parameter.Name
                    : memberExpression.Expression is MemberExpression innerMemberExpression
                        ? innerMemberExpression.Member.Name
                        : string.Empty;

                if (param == "<>h__TransparentIdentifier0" || FromAlias.StartsWith(DefaultAliasPrefix) || _aliasKeywords.Contains(param))
                {
                    // (1) the load argument was defined in a previous let statement, i.e :
                    //     let detailId = "details/1-A" 
                    //     let detail = session.Load<Detail>(detailId)
                    //     ...
                    // (2) OR the from-alias was a reserved word and we have a let statement,
                    //     so we changed it to "__ravenDefaultAlias".
                    //     the load-argument might be a path with respect to the original from-alias name.
                    // (3) OR the parameter name of the load argument is a reserved word
                    //     that was defined in a previous let statement, i.e : 
                    //     let update = session.Load<Order>("orders/1-A")
                    //     let employee = session.Load<Employee>(update.Employee)

                    //so we use js load() method (inside output function) instead of using a LoadToken

                    AppendLineToOutputFunction(name, ToJs(expression), wrapper);
                    return;
                }
                
                arg = ToJs(loadSupport.Arg, true);
            }
            else
            {
                //the argument is not a static string value nor a path, 
                //so we use js load() method (inside output function) instead of using a LoadToken
                AppendLineToOutputFunction(name, ToJs(expression), wrapper);
                return;
            }

            if (_loadTokens == null)
            {
                _loadTokens = new List<LoadToken>();
                _loadAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            name = RenameAliasIfNeeded(name);

            var indexOf = arg.IndexOf('.');
            if (indexOf != -1)
            {
                var loadFrom = arg.Substring(0, indexOf);
                if (_loadAliasesMovedToOutputFunction.Contains(loadFrom))
                {
                    //edge case: earlier we had 'Load(x.SomeId).member as alias1'                   
                    //so we did : load x.SomeId as _doc_i (LoadToken)
                    //then inside the output function we added:
                    //var alias1 = _doc_i.member;

                    //now we have 'Load(alias1.SomeId2) as alias2'
                    //since alias1 is defined only inside the output function,
                    //we add to the output function:
                    //var alias2 = load(alias1.SomeId2); (JS load() method)

                    AppendLineToOutputFunction(name, ToJs(expression), wrapper);
                    _loadAliasesMovedToOutputFunction.Add(name);
                    return;
                }
            }

            string alias;
            if (js == string.Empty)
            {
                alias = loadSupport.IsEnumerable
                    ? $"{name}[]"
                    : name;

                AddLoadToken(arg, alias);

                if (expression is MethodCallExpression mce)
                {
                    foreach (var type in mce.Method.GetGenericArguments())
                        _loadTypes.Add(type);
                }
                
                if (wrapper != null)
                {
                    AddPropertyToWrapperObject(alias, alias, wrapper);
                }
                return;
            }

            // here we have Load(id).member or Load(id).call, i.e : Load(u.Detail).Name, Load(u.Details).Select(x=>x.Name), etc.. 
            // so we add 'LOAD id as _doc_i' to the query
            // then inside the output function, we add 'var name = _doc_i.member;'

            string doc;
            if (loadSupport.IsEnumerable)
            {
                doc = $"_docs_{_loadTokens.Count}";
                alias = $"{doc}[]";
            }
            else
            {
                doc = $"_doc_{_loadTokens.Count}";
                alias = doc;
            }

            AddLoadToken(arg, alias);

            //add name to the list of aliases that were moved to the output function
            _loadAliasesMovedToOutputFunction.Add(name);

            if (js.StartsWith(".") == false && js.StartsWith("[") == false)
            {
                js = "." + js;
            }

            AppendLineToOutputFunction(name, doc + js, wrapper);
        }

        private bool IsDictionaryOnDictionaryExpression(Expression expression)
        {
            //We want to wrap expressions like doc.SomeDictionary.Values inside a JS declared function
            //because they are translated to JS map
            return expression is MemberExpression memberExpression && 
                   memberExpression.Expression.Type.GetInterfaces().Contains(typeof(IDictionary)) &&
                   memberExpression.Member.DeclaringType != null &&
                   memberExpression.Member.DeclaringType.GetInterfaces().Contains(typeof(IDictionary));
        }

        private void AddLoadToken(string arg, string alias)
        {
            _loadTokens.Add(LoadToken.Create(arg, alias));
            _loadAliases.Add(alias);
        }

        private void AppendLineToOutputFunction(string name, string js, StringBuilder wrapper = null)
        {
            name = RenameAliasIfReservedInJs(name);

            if (wrapper != null)
            {
                AddPropertyToWrapperObject(name, js, wrapper);
                return;
            }

            _declareBuilder ??= new StringBuilder();
            _declareBuilder.Append('\t')
                .Append("var ").Append(name)
                .Append(" = ").Append(js).Append(';')
                .Append(Environment.NewLine);
        }

        private static void AddPropertyToWrapperObject(string name, string js, StringBuilder wrapper)
        {
            wrapper.Append('\t')
                .Append(name)
                .Append(" : ").Append(js).Append(',')
                .Append(Environment.NewLine);
        }

        internal void AddFromAlias(string alias)
        {
            FromAlias = RenameAliasIfNeeded(alias);
            DocumentQuery.AddFromAliasToWhereTokens(FromAlias);
            DocumentQuery.AddFromAliasToFilterTokens(FromAlias);
            DocumentQuery.AddFromAliasToOrderByTokens(FromAlias);
        }

        private void AddAliasToIncludeTokensIfNeeded()
        {
            FromAlias = DocumentQuery.AddAliasToIncludesTokens(FromAlias);
        }

        private string RenameAliasIfNeeded(string alias)
        {
            if (_aliasKeywords.Contains(alias) == false)
                return RenameAliasIfReservedInJs(alias);

            if (_insideLet > 0)
            {
                var newAlias = $"{DefaultAliasPrefix}{_aliasesCount++}";
                AppendLineToOutputFunction(alias, newAlias);
                return newAlias;
            }

            return "'" + alias + "'";
        }

        private static string RenameAliasIfReservedInJs(string alias)
        {
            if (JavascriptConversionExtensions.ReservedWordsSupport.JsReservedWords.Contains(alias))
            {
                return "_" + alias;
            }

            return alias;
        }

        private string TranslateSelectBodyToJs(Expression expression)
        {
            var sb = new StringBuilder();
          
            if (expression is NewExpression newExpression)
            {
                sb.Append("{ ");

                for (int index = 0; index < newExpression.Arguments.Count; index++)
                {
                    var name = GetSelectPath(newExpression.Members[index]);

                    if (newExpression.Arguments[index] is MemberInitExpression or NewExpression)
                    {
                        if (index > 0)
                            sb.Append(", ");

                        _jsProjectionNames.Add(name);
                        sb.Append(name).Append(" : ").Append(TranslateSelectBodyToJs(newExpression.Arguments[index]));
                    }
                    else
                    {
                        AddJsProjection(name, newExpression.Arguments[index], sb, index != 0);
                    }
                }
                sb.Append(" }");
            }

            if (expression is MemberInitExpression mie)
            {
                sb.Append("{ ");

                for (int index = 0; index < mie.Bindings.Count; index++)
                {
                    var field = mie.Bindings[index] as MemberAssignment;
                    if (field == null)
                        continue;

                    var name = GetSelectPath(field.Member);
                    if (field.Expression is MemberInitExpression)
                    {
                        if (index > 0)
                            sb.Append(", ");

                        _jsProjectionNames.Add(name);
                        sb.Append(name).Append(" : ").Append(TranslateSelectBodyToJs(field.Expression));
                        continue;
                    }

                    AddJsProjection(name, field.Expression, sb, index != 0);

                }
                sb.Append(" }");
            }

            if (expression is MemberExpression or MethodCallExpression)
            { 
                if (IsRawOrTimeSeriesCall(expression, out string script) == false)
                    script = ToJs(expression);
                
                sb.Append(script);
            }

            return sb.ToString();
        }

        private void AddJsProjection(string name, Expression expression, StringBuilder sb, bool addComma)
        {
            _jsProjectionNames.Add(name);

            if (IsRawOrTimeSeriesCall(expression, out string script) == false)
            {
                script = ToJs(expression);
            }
            
            if (QueryGenerator.Conventions.FindProjectedPropertyNameForIndex != null)
            {
                var firstDotIndex = script.IndexOf('.');

                if (firstDotIndex != -1)
                {
                    var documentAlias = script.Substring(0, firstDotIndex);
                    var propertySelector = script.Substring(firstDotIndex + 1);

                    if (documentAlias == FromAlias || _loadAliases.Contains(documentAlias))
                    {
                        propertySelector = QueryGenerator.Conventions.FindProjectedPropertyNameForIndex(typeof(T), IndexName, CurrentPath,
                            propertySelector);

                        script = $"{documentAlias}.{propertySelector}";
                    }
                }
            }
            
            if (addComma)
            {
                sb.Append(", ");
            }

            sb.Append(name).Append(" : ").Append(script);
        }

        private static bool IsRawCall(MethodCallExpression mce)
        {
            return mce.Method.DeclaringType == typeof(RavenQuery) && mce.Method.Name == "Raw";
        }

        private string ToJs(Expression expression, bool loadArg = false, JavascriptConversionExtensions.LoadSupport loadSupport = null)
        {
            var extensions = new JavascriptConversionExtension[]
            {
                new JavascriptConversionExtensions.DictionarySupport(),
                JavascriptConversionExtensions.LinqMethodsSupport.Instance,
                loadSupport ?? new JavascriptConversionExtensions.LoadSupport(_loadTypes ??= new()),
                JavascriptConversionExtensions.MetadataSupport.Instance,
                JavascriptConversionExtensions.CompareExchangeSupport.Instance,
                JavascriptConversionExtensions.CounterSupport.Instance,
                new JavascriptConversionExtensions.WrappedConstantSupport<T>(DocumentQuery, _projectionParameters),
                JavascriptConversionExtensions.MathSupport.Instance,
                new JavascriptConversionExtensions.TransparentIdentifierSupport(),
                JavascriptConversionExtensions.ReservedWordsSupport.Instance,
                JavascriptConversionExtensions.InvokeSupport.Instance,
                JavascriptConversionExtensions.ToStringSupport.Instance,
                JavascriptConversionExtensions.DateTimeSupport.Instance,
                JavascriptConversionExtensions.TimeSpanSupport.Instance,
                JavascriptConversionExtensions.NullCoalescingSupport.Instance,
                JavascriptConversionExtensions.NestedConditionalSupport.Instance,
                JavascriptConversionExtensions.StringSupport.Instance,
                new JavascriptConversionExtensions.ConstSupport(DocumentQuery.Conventions),
                JavascriptConversionExtensions.ValueTypeParseSupport.Instance,
                JavascriptConversionExtensions.JsonPropertyAttributeSupport.Instance,
                JavascriptConversionExtensions.NullComparisonSupport.Instance,
                JavascriptConversionExtensions.NullableSupport.Instance,
                JavascriptConversionExtensions.NewSupport.Instance,
                JavascriptConversionExtensions.ListInitSupport.Instance,
                CustomMemberInitAsJson.ForAllTypes,
                new JavascriptConversionExtensions.TimeSeriesSupport<T>(this),
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                JavascriptConversionExtensions.DateOnlySupport.Instance
#endif
            };

            if (loadArg == false)
            {
                _loadTypes ??= new();
                var newSize = _typedParameterSupport != null
                    ? extensions.Length + 2
                    : extensions.Length + 1;
                Array.Resize(ref extensions, newSize);
                if (_typedParameterSupport != null)
                    extensions[newSize - 2] = _typedParameterSupport;
                extensions[newSize - 1] = new JavascriptConversionExtensions.IdentityPropertySupport(DocumentQuery.Conventions, _typedParameterSupport?.Name, _originalQueryType, _loadTypes);
            }

            return expression.CompileToJavascript(new JavascriptCompilationOptions(ScriptVersion.ECMAScript2017, extensions)
            {
                CustomMetadataProvider = new PropertyNameConventionJSMetadataProvider(_conventions)
            });
        }

        private bool IsRawOrTimeSeriesCall(Expression expression, out string script)
        {
            script = null;
            if (!(expression is MethodCallExpression mce))
                return false;
            
            if (IsRawCall(mce))
            {

                if (typeof(T).IsPrimitive || typeof(T) == typeof(string))
                    throw new NotSupportedException($"Unsupported parameter type {expression.Type.Name}. Primitive types/string types are not allowed in Raw<T>() method.");

                if (mce.Arguments.Count == 1)
                {
                    script = (mce.Arguments[0] as ConstantExpression)?.Value.ToString();
                }
                else
                {
                    var path = ToJs(mce.Arguments[0]);
                    var raw = (mce.Arguments[1] as ConstantExpression)?.Value.ToString();

                    script = $"{path}.{raw}";
                }

            }
            else if (LinqPathProvider.IsTimeSeriesCall(mce))
            {
                script = GenerateTimeSeriesScript(mce);
            }

            return script != null;
        }

        internal string GenerateTimeSeriesScript(MethodCallExpression mce)
        {
            var visitor = new TimeSeriesQueryVisitor<T>(this);
            var tsQueryText = visitor.Visit(mce);

            var paramsBuilder = new StringBuilder();

            for (int i = 0; i < visitor.Parameters?.Count; i++)
            {
                if (i > 0)
                    paramsBuilder.Append(", ");
                paramsBuilder.Append(visitor.Parameters[i]);
            }

            var parameters = paramsBuilder.ToString();

            _declareTokens ??= new List<DeclareToken>();

            var tsFunctionName = Constants.TimeSeries.QueryFunction + _declareTokens.Count;

            _declareTokens.Add(DeclareToken.CreateTimeSeries(tsFunctionName, tsQueryText, parameters));

            return $"{tsFunctionName}({parameters})";
        }

        private static bool HasComputation(MemberExpression memberExpression)
        {
            if (memberExpression.Member.Name == "HasValue" &&
                memberExpression.Expression.Type.IsNullableType())
            {
                return true;
            }

            var cur = memberExpression;

            while (cur != null)
            {
                switch (cur.Expression?.NodeType)
                {
                    case ExpressionType.Call:
                    case ExpressionType.Invoke:
                    case ExpressionType.Add:
                    case ExpressionType.And:
                    case ExpressionType.AndAlso:
                    case ExpressionType.AndAssign:
                    case ExpressionType.Decrement:
                    case ExpressionType.Increment:
                    case ExpressionType.PostDecrementAssign:
                    case ExpressionType.PostIncrementAssign:
                    case ExpressionType.PreDecrementAssign:
                    case ExpressionType.PreIncrementAssign:
                    case ExpressionType.AddAssign:
                    case ExpressionType.AddAssignChecked:
                    case ExpressionType.AddChecked:
                    case ExpressionType.Index:
                    case ExpressionType.Assign:
                    case ExpressionType.Block:
                    case ExpressionType.Conditional:
                    case ExpressionType.ArrayIndex:
                    case null:
                        return true;
                }
                cur = cur.Expression as MemberExpression;
            }
            return false;
        }

        private static bool TryGetMemberExpression(Expression expression, out MemberExpression member)
        {
            if (expression is UnaryExpression unaryExpression)
                return TryGetMemberExpression(unaryExpression.Operand, out member);

            if (expression is LambdaExpression lambdaExpression)
                return TryGetMemberExpression(lambdaExpression.Body, out member);

            member = expression as MemberExpression;
            return member != null;
        }

        private void VisitSelectAfterGroupBy(Expression operand, Expression elementSelectorPath)
        {
            if (DocumentQuery.IsDynamicMapReduce == false)
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
                        DocumentQuery.GroupByKey(null, GetSelectPath(fieldMember));
                    }
                    break;
                case ExpressionType.MemberAccess:

                    var keyExpression = (MemberExpression)fieldExpression;
                    var name = GetSelectPath(keyExpression);

                    if ("Key".Equals(name, StringComparison.Ordinal))
                    {
                        var projectedName = ExtractProjectedName(fieldMember);

                        if (projectedName.Equals("Key", StringComparison.Ordinal))
                            DocumentQuery.GroupByKey(null);
                        else
                            DocumentQuery.GroupByKey(null, projectedName);
                    }
                    else if (name.StartsWith("Key.", StringComparison.Ordinal))
                    {
                        var compositeGroupBy = name.Split('.');

                        if (compositeGroupBy.Length > 2)
                            throw new NotSupportedException("Nested fields inside composite GroupBy keys are not supported");

                        var fieldName = compositeGroupBy[1];
                        var projectedName = ExtractProjectedName(fieldMember);

                        DocumentQuery.GroupByKey(fieldName, projectedName);
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

                    if (methodCallExpression != null)
                    {
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
                    else
                    {
                        var unaryExpression = lambdaExpression.Body as UnaryExpression;

                        if (unaryExpression == null)
                            throw new NotSupportedException("No idea how to handle this dynamic map-reduce query!");

                        switch (unaryExpression.NodeType)
                        {
                            case ExpressionType.ArrayLength:
                                var prop = GetMember(unaryExpression.Operand);

                                mapReduceField = prop.Path + ".Length";
                                break;
                            default:
                                throw new NotSupportedException($"Unhandled expression type: {unaryExpression.NodeType}");
                        }
                    }
                }
            }

            if (mapReduceOperation == AggregationOperation.Count)
            {
                if (mapReduceField.Equals("Count", StringComparison.Ordinal))
                    DocumentQuery.GroupByCount();
                else
                    DocumentQuery.GroupByCount(mapReduceField);
                return;
            }

            if (mapReduceOperation == AggregationOperation.Sum)
            {
                DocumentQuery.GroupBySum(mapReduceField, renamedField);
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
            HandleKeywordsIfNeeded(ref field, ref alias);

            var identityProperty = DocumentQuery.Conventions.GetIdentityProperty(_ofType ?? _originalQueryType);
            var identityConverted = DocumentQuery.Conventions.GetConvertedPropertyNameFor(identityProperty) ?? identityProperty?.Name;

            if (identityProperty != null && identityConverted  == field)
            {
                FieldsToFetch.Add(new FieldToFetch(Constants.Documents.Indexing.Fields.DocumentIdFieldName, alias));
                return;
            }

            FieldsToFetch.Add(new FieldToFetch(field, alias));
        }

        private void HandleKeywordsIfNeeded(ref string field, ref string alias)
        {
            if (field != null)
            {
                if (NeedToAddFromAliasToField(field))
                {
                    AddFromAliasToFieldToFetch(ref field, ref alias);
                }
                else if (_aliasKeywords.Contains(field))
                {
                    AddDefaultAliasToQuery();
                    AddFromAliasToFieldToFetch(ref field, ref alias, true);
                }

                var indexOf = field.IndexOf(".", StringComparison.OrdinalIgnoreCase);
                if (indexOf != -1)
                {
                    var parameter = field.Substring(0, indexOf);
                    if (_aliasKeywords.Contains(parameter))
                    {
                        // field is a nested path that starts with RQL keyword,
                        // need to quote the keyword

                        var nestedPath = field.Substring(indexOf + 1);
                        AddDefaultAliasToQuery();
                        AddFromAliasToFieldToFetch(ref parameter, ref alias, true);
                        field = $"{parameter}.{nestedPath}";
                    }
                }
            }

            if (string.Equals(alias, "load", StringComparison.OrdinalIgnoreCase) ||
                _aliasKeywords.Contains(alias))
            {
                alias = "'" + alias + "'";
            }
        }
        private bool NeedToAddFromAliasToField(string field)
        {
            return _addedDefaultAlias && FieldTypeAllowToAddAlias(field);
        }

        private bool FieldTypeAllowToAddAlias(string field)
        {
            return field.StartsWith($"{FromAlias}.") == false &&
                field.StartsWith("id(") == false &&
                field.StartsWith("counter(") == false &&
                field.StartsWith("counterRaw(") == false &&
                field.StartsWith("cmpxchg(") == false;
        }
        
        private void AddFromAliasToFieldToFetch(ref string field, ref string alias, bool quote = false)
        {
            if (field == alias)
            {
                alias = null;
            }

            if (FieldTypeAllowToAddAlias(field) == false)
                return;

            field = quote
                ? $"{FromAlias}.'{field}'"
                : $"{FromAlias}.{field}";
        }

        private void AddDefaultAliasToQuery()
        {
            var fromAlias = $"{DefaultAliasPrefix}{_aliasesCount++}";
            AddFromAlias(fromAlias);
            foreach (var fieldToFetch in FieldsToFetch)
            {
                if (FieldTypeAllowToAddAlias(fieldToFetch.Name))
                {
                    fieldToFetch.Name = $"{fromAlias}.{fieldToFetch.Name}";
                }
            }

            _addedDefaultAlias = true;
        }

        private void VisitSkip(ConstantExpression constantExpression)
        {
            if (constantExpression.Value.GetType() == typeof(int))
                DocumentQuery.Skip((int)constantExpression.Value);
            else
                DocumentQuery.Skip((long)constantExpression.Value);
        }

        private void VisitTake(ConstantExpression constantExpression)
        {
            //Don't have to worry about the cast failing, the Take() extension method only takes an int
            DocumentQuery.Take((int)constantExpression.Value);
        }

        private void VisitAll(Expression<Func<T, bool>> predicateExpression)
        {
            throw new NotSupportedException("All() is not supported for linq queries");
        }

        private void VisitAny()
        {
            DocumentQuery.Take(1);
            _queryType = SpecialQueryType.Any;
        }

        private void VisitCount()
        {
            DocumentQuery.Take(0);
            _queryType = SpecialQueryType.Count;
        }

        private void VisitLongCount()
        {
            DocumentQuery.Take(0);
            _queryType = SpecialQueryType.LongCount;
        }

        private void VisitSingle()
        {
            DocumentQuery.Take(2);
            _queryType = SpecialQueryType.Single;
        }

        private void VisitSingleOrDefault()
        {
            DocumentQuery.Take(2);
            _queryType = SpecialQueryType.SingleOrDefault;
        }

        private void VisitFirst()
        {
            DocumentQuery.Take(1);
            _queryType = SpecialQueryType.First;
        }

        private void VisitFirstOrDefault()
        {
            DocumentQuery.Take(1);
            _queryType = SpecialQueryType.FirstOrDefault;
        }

        private string GetFieldNameForRangeQuery(ExpressionInfo expression, object value)
        {
            var identityProperty = DocumentQuery.Conventions.GetIdentityProperty(typeof(T));
            var identityConverted = DocumentQuery.Conventions.GetConvertedPropertyNameFor(identityProperty);

            if (identityProperty != null && identityConverted == expression.Path)
            {
                if (identityProperty.Type() == typeof(int) ||
                    identityProperty.Type() == typeof(long) ||
                    identityProperty.Type() == typeof(double) ||
                    identityProperty.Type() == typeof(float) ||
                    identityProperty.Type() == typeof(Guid) ||
                    identityProperty.Type() == typeof(decimal))
                {
                    throw new NotSupportedException("You cannot issue range queries on a identity property that is of a numeric type." + Environment.NewLine +
                                                    "RavenDB numeric ids are purely client side, on the server, they are always strings, " +
                                                    "and aren't going to sort according to your expectations." + Environment.NewLine +
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
        public IDocumentQuery<T> GetDocumentQueryFor(Expression expression, Action<IAbstractDocumentQuery<T>> customization = null)
        {
            var documentQuery = (DocumentQuery<T>)QueryGenerator.Query<T>(IndexName, _collectionName, _isMapReduce);
            if (_afterQueryExecuted != null)
                documentQuery.AfterQueryExecuted(_afterQueryExecuted);

            DocumentQuery = (IAbstractDocumentQuery<T>)documentQuery;
            customization?.Invoke(DocumentQuery);

            if (_highlightings != null)
            {
                foreach (var highlighting in _highlightings.Highlightings)
                    DocumentQuery.Highlight(highlighting.FieldName, highlighting.FragmentLength, highlighting.FragmentCount, highlighting.Options, out _);
            }

            try
            {
                VisitExpression(expression);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("Could not understand expression: " + expression, e);
            }
            catch (NotSupportedException e)
                // we filter this so we'll get better errors
                when (e.HelpLink != "DoNotWrap")
            {
                throw new NotSupportedException("Could not understand expression: " + expression, e);
            }

            _customizeQuery?.Invoke((IDocumentQueryCustomization)DocumentQuery);

            AddAliasToIncludeTokensIfNeeded();

            if (_jsSelectBody != null)
            {
                return documentQuery.CreateDocumentQueryInternal<T>(new QueryData(new[] { _jsSelectBody }, _jsProjectionNames, FromAlias, _declareTokens, _loadTokens, true)
                {
                    QueryStatistics = _queryStatistics
                });
            }

            var (fields, projections) = GetProjections();

            return documentQuery.CreateDocumentQueryInternal<T>(new QueryData(fields, projections, FromAlias, null, _loadTokens)
            {
                IsProjectInto = _isProjectInto,
                QueryStatistics = _queryStatistics
            });
        }

        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        public IAsyncDocumentQuery<T> GetAsyncDocumentQueryFor(Expression expression, Action<IAbstractDocumentQuery<T>> customization = null)
        {
            var asyncDocumentQuery = (AsyncDocumentQuery<T>)QueryGenerator.AsyncQuery<T>(IndexName, _collectionName, _isMapReduce);
            if (_afterQueryExecuted != null)
                asyncDocumentQuery.AfterQueryExecuted(_afterQueryExecuted);

            DocumentQuery = (IAbstractDocumentQuery<T>)asyncDocumentQuery;
            customization?.Invoke(DocumentQuery);

            if (_highlightings != null)
            {
                foreach (var highlighting in _highlightings.Highlightings)
                    DocumentQuery.Highlight(highlighting.FieldName, highlighting.FragmentLength, highlighting.FragmentCount, highlighting.Options, out _);
            }

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

            AddAliasToIncludeTokensIfNeeded();

            if (_jsSelectBody != null)
            {
                return asyncDocumentQuery.CreateDocumentQueryInternal<T>(new QueryData(new[] { _jsSelectBody }, _jsProjectionNames, FromAlias, _declareTokens, _loadTokens, true)
                {
                    QueryStatistics = _queryStatistics
                });
            }

            var (fields, projections) = GetProjections();

            return asyncDocumentQuery.CreateDocumentQueryInternal<T>(new QueryData(fields, projections, FromAlias, null, _loadTokens)
            {
                IsProjectInto = _isProjectInto,
                QueryStatistics = _queryStatistics
            });
        }

        /// <summary>
        /// Executes the specified expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public object Execute(Expression expression)
        {
            _chainedWhere = false;
            if (_isProjectInto == false)
                FieldsToFetch?.Clear();
            DocumentQuery = (IAbstractDocumentQuery<T>)GetDocumentQueryFor(expression);
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
                var fieldName = fieldToFetch.Name;
                var fieldAlias = fieldToFetch.Alias;

                if (_isProjectInto)
                {
                    HandleKeywordsIfNeeded(ref fieldName, ref fieldAlias);
                }
                
                fields[i] = fieldName;
                projections[i] = fieldAlias ?? fieldName;

                i++;
            }

            return (fields, projections);
        }

        private object ExecuteQuery<TProjection>()
        {
            var (fields, projections) = GetProjections();

            // used only for DocumentQuery
            var finalQuery = ((DocumentQuery<T>)DocumentQuery).CreateDocumentQueryInternal<TProjection>(
                new QueryData(fields, projections, FromAlias, _declareTokens, _loadTokens, _declareTokens != null || _jsSelectBody != null)
                {
                    IsProjectInto = _isProjectInto
                });

            var executeQuery = GetQueryResult(finalQuery);

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
                        var qr = finalQuery.GetQueryResult();
                        if (_queryType == SpecialQueryType.Count)
                        {
                            if (qr.TotalResults > int.MaxValue)
                                DocumentSession.ThrowWhenResultsAreOverInt32(qr.TotalResults, "Count", "LongCount");
                            return (int)qr.TotalResults;
                        }
                        return qr.TotalResults;
                    }
                default:
                    {
                        return finalQuery;
                    }
            }
        }

        private static bool IsCollection(Type type)
        {
            return type.IsArray || typeof(IEnumerable).IsAssignableFrom(type) && typeof(string) != type;
        }

        #region Nested type: SpecialQueryType

        /// <summary>
        /// Different query types 
        /// </summary>
        private enum SpecialQueryType
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

    public sealed class FieldToFetch
    {
        public FieldToFetch(string name, string alias)
        {
            Name = name;
            Alias = name != alias ? alias : null;
        }

        public string Name { get; internal set; }
        public string Alias { get; internal set; }

        private bool Equals(FieldToFetch other)
        {
            return string.Equals(Name, other.Name) && string.Equals(Alias, other.Alias, StringComparison.Ordinal);
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
                return ((Name != null ? StringComparer.Ordinal.GetHashCode(Name) : 0) * 397) ^ (Alias != null ? StringComparer.Ordinal.GetHashCode(Alias) : 0);
            }
        }
    }
}
