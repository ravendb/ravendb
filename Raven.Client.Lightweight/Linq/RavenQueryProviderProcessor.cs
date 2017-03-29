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
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Json.Linq;

namespace Raven.Client.Linq
{
    /// <summary>
    /// Process a Linq expression to a Lucene query
    /// </summary>
    public class RavenQueryProviderProcessor<T>
    {
        private readonly Action<IDocumentQueryCustomization> customizeQuery;
        /// <summary>
        /// The query generator
        /// </summary>
        protected readonly IDocumentQueryGenerator queryGenerator;
        private readonly Action<QueryResult> afterQueryExecuted;
        private readonly Action<RavenJObject> afterStreamExcuted;
        private bool chainedWhere;
        private int insideWhere;
        private IAbstractDocumentQuery<T> documentQuery;
        private SpecialQueryType queryType = SpecialQueryType.None;
        private Type newExpressionType;
        private string currentPath = string.Empty;
        private int subClauseDepth;
        private string resultsTransformer;
        private readonly Dictionary<string, RavenJToken> transformerParameters;

        private LinqPathProvider linqPathProvider;
        /// <summary>
        /// The index name
        /// </summary>
        protected readonly string indexName;

        /// <summary>
        /// Gets the current path in the case of expressions within collections
        /// </summary>
        public string CurrentPath
        {
            get { return currentPath; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryProviderProcessor{T}"/> class.
        /// </summary>
        /// <param name="queryGenerator">The document query generator.</param>
        /// <param name="customizeQuery">The customize query.</param>
        /// <param name="afterQueryExecuted">Executed after the query run, allow access to the query results</param>
        /// <param name="afterStreamExecuted">Executed after the stream run, allow access to the stream results</param>
        /// <param name="indexName">The name of the index the query is executed against.</param>
        /// <param name="fieldsToFetch">The fields to fetch in this query</param>
        /// <param name="fieldsTRename">The fields to rename for the results of this query</param>
        /// <param name="isMapReduce"></param>
        /// <param name="resultsTransformer"></param>
        /// <param name="transformerParameters"></param>
        /// <param name ="originalType" >the original type of the query if TransformWith is called otherwise null</param>
        public RavenQueryProviderProcessor(IDocumentQueryGenerator queryGenerator, Action<IDocumentQueryCustomization> customizeQuery, Action<QueryResult> afterQueryExecuted, 
            Action<RavenJObject> afterStreamExecuted,  string indexName, HashSet<string> fieldsToFetch, List<RenamedField> fieldsTRename, bool isMapReduce, string resultsTransformer,
            Dictionary<string, RavenJToken> transformerParameters,Type originalType)
        {
            FieldsToFetch = fieldsToFetch;
            FieldsToRename = fieldsTRename;
            newExpressionType = typeof(T);
            this.queryGenerator = queryGenerator;
            this.indexName = indexName;
            this.isMapReduce = isMapReduce;
            this.afterQueryExecuted = afterQueryExecuted;
            this.afterStreamExcuted = afterStreamExecuted;
            this.customizeQuery = customizeQuery;
            this.resultsTransformer = resultsTransformer;
            this.transformerParameters = transformerParameters;
            this.originalQueryType = originalType;
            linqPathProvider = new LinqPathProvider(queryGenerator.Conventions);
        }

        /// <summary>
        /// Gets or sets the fields to fetch.
        /// </summary>
        /// <value>The fields to fetch.</value>
        public HashSet<string> FieldsToFetch { get; set; }

        /// <summary>
        /// Rename the fields from one name to another
        /// </summary>
        public List<RenamedField> FieldsToRename { get; set; }

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
                                documentQuery.OpenSubclause();
                                documentQuery.Where("*:*");
                                documentQuery.AndAlso();
                                documentQuery.NegateNext();
                                VisitMethodCall((MethodCallExpression)unaryExpressionOp, negated: true);
                                documentQuery.CloseSubclause();
                                break;
                            default:
                                //probably the case of !(complex condition)
                                documentQuery.OpenSubclause();
                                documentQuery.Where("*:*");
                                documentQuery.AndAlso();
                                documentQuery.NegateNext();
                                VisitExpression(unaryExpressionOp);
                                documentQuery.CloseSubclause();
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


            if (subClauseDepth > 0) documentQuery.OpenSubclause();
            subClauseDepth++;

            // negate optimization : (RavenDB-3973).  in order to disable you may just set isNotEqualCheckBoundsToAndAlsoLeft & Right to "false" 
            bool isNotEqualCheckBoundsToAndAlsoLeft = (andAlso.Left.NodeType == ExpressionType.NotEqual);
            bool isNotEqualCheckBoundsToAndAlsoRight = (andAlso.Right.NodeType == ExpressionType.NotEqual);

            if (isNotEqualCheckBoundsToAndAlsoRight && isNotEqualCheckBoundsToAndAlsoLeft)
                // avoid empty group (i.e. : "a != 1 && a != 2"  should generate "((-a:1 AND a:*) AND -a:2)"
                isNotEqualCheckBoundsToAndAlsoLeft = false;

            if (isNotEqualCheckBoundsToAndAlsoLeft || isNotEqualCheckBoundsToAndAlsoRight)
            {
                subClauseDepth++;
                documentQuery.OpenSubclause();
            }
            isNotEqualCheckBoundsToAndAlso = isNotEqualCheckBoundsToAndAlsoLeft;
            VisitExpression(andAlso.Left);
            documentQuery.AndAlso();
            isNotEqualCheckBoundsToAndAlso = isNotEqualCheckBoundsToAndAlsoRight;
            VisitExpression(andAlso.Right);
            isNotEqualCheckBoundsToAndAlso = false;

            if (isNotEqualCheckBoundsToAndAlsoLeft || isNotEqualCheckBoundsToAndAlsoRight)
            {
                subClauseDepth--;
                documentQuery.CloseSubclause();
            }
            

            subClauseDepth--;
            if (subClauseDepth > 0) documentQuery.CloseSubclause();
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
                documentQuery.WhereBetweenOrEqual(leftMember.Item1.Path, min, max);
            else
                documentQuery.WhereBetween(leftMember.Item1.Path, min, max);

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
            return linqPathProvider.GetValueFromExpression(expression, type);
        }

        private void VisitOrElse(BinaryExpression orElse)
        {
            if (subClauseDepth > 0) documentQuery.OpenSubclause();
            subClauseDepth++;

            VisitExpression(orElse.Left);
            documentQuery.OrElse();
            VisitExpression(orElse.Right);

            subClauseDepth--;
            if (subClauseDepth > 0) documentQuery.CloseSubclause();
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
                documentQuery.OpenSubclause();
                documentQuery.Where("*:*");
                documentQuery.AndAlso();
                documentQuery.NegateNext();
                VisitExpression(expression.Left);
                documentQuery.CloseSubclause();
                return;
            }

            var methodCallExpression = expression.Left as MethodCallExpression;
            // checking for VB.NET string equality
            if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" &&
                expression.Right.NodeType == ExpressionType.Constant &&
                Equals(((ConstantExpression)expression.Right).Value, 0))
            {
                var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);

                documentQuery.WhereEquals(
                    new WhereParams
                    {
                        FieldName = expressionMemberInfo.Path,
                        Value = GetValueFromExpression(methodCallExpression.Arguments[1], GetMemberType(expressionMemberInfo)),
                        IsAnalyzed = true,
                        AllowWildcards = false
                    });
                return;
            }

            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitEquals(Expression.Equal(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);

            documentQuery.WhereEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
                IsAnalyzed = true,
                AllowWildcards = false,
                IsNestedPath = memberInfo.IsNestedPath
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
                documentQuery.OpenSubclause();
                documentQuery.NegateNext();
                documentQuery.WhereEquals(new WhereParams
                {
                    FieldName = expressionMemberInfo.Path,
                    Value = GetValueFromExpression(methodCallExpression.Arguments[0], GetMemberType(expressionMemberInfo)),
                    IsAnalyzed = true,
                    AllowWildcards = false
                });
                documentQuery.AndAlso();
                documentQuery
                    .WhereEquals(new WhereParams
                    {
                        FieldName = expressionMemberInfo.Path,
                        Value = "*",
                        IsAnalyzed = true,
                        AllowWildcards = true
                    });
                documentQuery.CloseSubclause();
                return;
            }

            if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
            {
                VisitNotEquals(Expression.NotEqual(expression.Right, expression.Left));
                return;
            }

            var memberInfo = GetMember(expression.Left);
            if (isNotEqualCheckBoundsToAndAlso == false)
                documentQuery.OpenSubclause();
            documentQuery.NegateNext();
            documentQuery.WhereEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
                IsAnalyzed = true,
                AllowWildcards = false
            });
            if (isNotEqualCheckBoundsToAndAlso == false)
            {
                documentQuery.AndAlso();
                documentQuery.WhereEquals(new WhereParams
                {
                    FieldName = memberInfo.Path,
                    Value = "*",
                    IsAnalyzed = true,
                    AllowWildcards = true
                });
                documentQuery.CloseSubclause();
            }
        }

        private static Type GetMemberType(ExpressionInfo info)
        {
            return info.Type;
        }

        private static readonly Regex castingRemover = new Regex(@"(?<!\\)[\(\)]",
                RegexOptions.Compiled
            );

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
                if (currentPath.EndsWith(","))
                    currentPath = currentPath.Substring(0, currentPath.Length - 1);
                return new ExpressionInfo(currentPath, parameterExpression.Type, false);
            }

            return GetMemberDirect(expression);
        }

        private ExpressionInfo GetMemberDirect(Expression expression)
        {
            var result = linqPathProvider.GetPath(expression);

            //for standard queries, we take just the last part. But for dynamic queries, we take the whole part
            result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);
            result.Path = castingRemover.Replace(result.Path, ""); // removing cast remains

            if (expression.NodeType == ExpressionType.ArrayLength)
                result.Path += ".Length";

            var propertyName = indexName == null || indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
                                   ? queryGenerator.Conventions.FindPropertyNameForDynamicIndex(typeof(T), indexName, CurrentPath,
                                                                                                result.Path)
                                   : queryGenerator.Conventions.FindPropertyNameForIndex(typeof(T), indexName, CurrentPath,
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
                        fieldInfo = new ExpressionInfo(currentPath.Substring(0, currentPath.Length - 1), expression.Object.Type,
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
#if !DNXCORE50
                    case StringComparison.InvariantCulture:
#endif
                    case StringComparison.Ordinal:
                        throw new NotSupportedException(
                            "RavenDB queries case sensitivity is dependent on the index, not the query. If you need case sensitive queries, use a static index and an NotAnalyzed field for that.");
                    case StringComparison.CurrentCultureIgnoreCase:
#if !DNXCORE50
                    case StringComparison.InvariantCultureIgnoreCase:
#endif
                    case StringComparison.OrdinalIgnoreCase:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            

            documentQuery.WhereEquals(new WhereParams
            {
                FieldName = fieldInfo.Path,
                Value = GetValueFromExpression(constant, GetMemberType(fieldInfo)),
                IsAnalyzed = true,
                AllowWildcards = false
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

            documentQuery.WhereStartsWith(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
        }

        private void VisitEndsWith(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Object);

            documentQuery.WhereEndsWith(
                memberInfo.Path,
                GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
        }

        private void VisitIsNullOrEmpty(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);

            documentQuery.OpenSubclause();
            documentQuery.WhereEquals(memberInfo.Path, Constants.NullValue, false);
            documentQuery.OrElse();
            documentQuery.WhereEquals(memberInfo.Path, Constants.EmptyString, false);
            documentQuery.CloseSubclause();
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

            documentQuery.WhereGreaterThan(
                GetFieldNameForRangeQuery(memberInfo, value),
                value);
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

            documentQuery.WhereGreaterThanOrEqual(
                GetFieldNameForRangeQuery(memberInfo, value),
                value);
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

            documentQuery.WhereLessThan(
                GetFieldNameForRangeQuery(memberInfo, value),
                value);
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

            documentQuery.WhereLessThanOrEqual(
                GetFieldNameForRangeQuery(memberInfo, value),
                value);
        }

        private void VisitAny(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);
            if (expression.Arguments.Count >= 2)
            {
                var oldPath = currentPath;
                currentPath = memberInfo.Path + ",";
                VisitExpression(expression.Arguments[1]);
                currentPath = oldPath;
            }
            else
            {
                // Support for .Where(x => x.Properties.Any())
                documentQuery.WhereEquals(new WhereParams
                {
                    FieldName = memberInfo.Path,
                    Value = "*",
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    IsNestedPath = memberInfo.IsNestedPath,
                });
            }
        }

        private void VisitContains(MethodCallExpression expression)
        {
            var memberInfo = GetMember(expression.Arguments[0]);
            var containsArgument = expression.Arguments[1];

            documentQuery.WhereEquals(new WhereParams
            {
                FieldName = memberInfo.Path,
                Value = GetValueFromExpression(containsArgument, containsArgument.Type),
                IsAnalyzed = true,
                AllowWildcards = false
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
                        documentQuery.OpenSubclause();
                        documentQuery.Where("*:*");
                        documentQuery.AndAlso();
                        documentQuery.NegateNext();
                    }
                    documentQuery.WhereEquals(new WhereParams
                    {
                        FieldName = memberInfo.Path,
                        Value = null,
                        IsAnalyzed = true,
                        AllowWildcards = false
                    });
                    if (boolValue)
                    {
                        documentQuery.CloseSubclause();
                    }
                }
                else
                {
                    memberInfo = GetMember(memberExpression);

                    documentQuery.WhereEquals(new WhereParams
                    {
                        FieldName = memberInfo.Path,
                        Value = boolValue,
                        IsAnalyzed = true,
                        AllowWildcards = false
                    });
                }
            }
            else if (memberExpression.Type == typeof(string))
            {
                if (currentPath.EndsWith(","))
                    currentPath = currentPath.Substring(0, currentPath.Length - 1);

                var memberInfo = GetMember(memberExpression);

                documentQuery.WhereEquals(new WhereParams
                {
                    FieldName = currentPath,
                    Value = GetValueFromExpression(memberExpression, GetMemberType(memberInfo)),
                    IsAnalyzed = true,
                    AllowWildcards = false,
                    IsNestedPath = memberInfo.IsNestedPath
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

            if (declaringType.IsGenericType())
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
                    documentQuery.AddOrder(Constants.TemporaryScoreValue, false);
                    VisitExpression(expression.Arguments[0]);
                    break;
                case "OrderByScoreDescending":
                    documentQuery.AddOrder(Constants.TemporaryScoreValue, true);
                    VisitExpression(expression.Arguments[0]);
                    break;
                case "Intersect":
                    VisitExpression(expression.Arguments[0]);
                    documentQuery.Intersect();
                    chainedWhere = false;
                    break;
                case "In":
                    var memberInfo = GetMember(expression.Arguments[0]);
                    var objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    documentQuery.WhereIn(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
                    break;
                case "ContainsAny":
                    memberInfo = GetMember(expression.Arguments[0]);
                    objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    documentQuery.ContainsAny(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
                    break;
                case "ContainsAll":
                    memberInfo = GetMember(expression.Arguments[0]);
                    objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
                    documentQuery.ContainsAll(memberInfo.Path, ((IEnumerable)objects).Cast<object>());
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
                documentQuery.OpenSubclause();
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
                if (chainedWhere && options.HasFlag(SearchOptions.And))
                {
                    documentQuery.AndAlso();
                }
                if (options.HasFlag(SearchOptions.Not))
                {
                    documentQuery.OpenSubclause();
                    documentQuery.NegateNext();
                }

                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[5], out value) == false)
                {
                    throw new InvalidOperationException("Could not extract value from " + expression);
                }
                var queryOptions = (EscapeQueryOptions)value;
                documentQuery.Search(expressionInfo.Path, searchTerms, queryOptions);
                if (options.HasFlag(SearchOptions.Not))
                {
                    documentQuery.AndAlso();
                    documentQuery.Search(expressionInfo.Path, "*");
                    documentQuery.CloseSubclause();
                }

                documentQuery.Boost(boost);

                if (options.HasFlag(SearchOptions.And))
                {
                    chainedWhere = true;
                }
            }

            if (expressions.Count > 1)
            {
                documentQuery.CloseSubclause();
            }

            if (LinqPathProvider.GetValueFromExpressionWithoutConversion(searchExpression.Arguments[4], out value) == false)
            {
                throw new InvalidOperationException("Could not extract value from " + searchExpression);
            }

            if (((SearchOptions)value).HasFlag(SearchOptions.Guess))
                chainedWhere = true;
        }

        private void VisitListMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "Contains":
                {
                    var memberInfo = GetMember(expression.Object);
        
                        var containsArgument = expression.Arguments[0];

                        documentQuery.WhereEquals(new WhereParams
                        {
                            FieldName = memberInfo.Path,
                            Value = GetValueFromExpression(containsArgument, containsArgument.Type),
                            IsAnalyzed = true,
                            AllowWildcards = false
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
                            documentQuery.OpenSubclause();
                            documentQuery.Where("*:*");
                            documentQuery.AndAlso();
                            documentQuery.NegateNext();
                        VisitIsNullOrEmpty(expression);
                            documentQuery.CloseSubclause();
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
                    insideWhere++;
                    VisitExpression(expression.Arguments[0]);
                    if (chainedWhere)
                    {
                            documentQuery.AndAlso();
                            documentQuery.OpenSubclause();
                    }
                    if (chainedWhere == false && insideWhere > 1)
                            documentQuery.OpenSubclause();
                        VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                    if (chainedWhere == false && insideWhere > 1)
                            documentQuery.CloseSubclause();
                    if (chainedWhere)
                            documentQuery.CloseSubclause();
                    chainedWhere = true;
                    insideWhere--;
                    break;
                }
                case "Select":
                {
                    if (expression.Arguments[0].Type.IsGenericType() &&
                            expression.Arguments[0].Type.GetGenericTypeDefinition() == typeof(IQueryable<>) &&
                        expression.Arguments[0].Type != expression.Arguments[1].Type)
                    {
                            documentQuery.AddRootType(expression.Arguments[0].Type.GetGenericArguments()[0]);
                    }
                    VisitExpression(expression.Arguments[0]);
                        VisitSelect(((UnaryExpression)expression.Arguments[1]).Operand);
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
                        if (chainedWhere)
                                documentQuery.AndAlso();
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
                    chainedWhere = chainedWhere || expression.Arguments.Count == 2;
                    break;
                }
                case "Single":
                case "SingleOrDefault":
                {
                    VisitExpression(expression.Arguments[0]);
                    if (expression.Arguments.Count == 2)
                    {
                        if (chainedWhere)
                                documentQuery.AndAlso();

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
                    chainedWhere = chainedWhere || expression.Arguments.Count == 2;
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
                        if (chainedWhere)
                                documentQuery.AndAlso();
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
                            if (chainedWhere)
                                documentQuery.AndAlso();
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
                        if (chainedWhere)
                                documentQuery.AndAlso();
                            VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                    }

                    VisitLongCount();
                    break;
                }
                case "Distinct":
                    if (expression.Arguments.Count == 1)
                    {
                        documentQuery.Distinct();
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
                default:
                {
                    throw new NotSupportedException("Method not supported: " + expression.Method.Name);
                }
            }
        }

        private void VisitOrderBy(LambdaExpression expression, bool descending)
        {
            var result = GetMemberDirect(expression.Body);

            var fieldType = result.Type;
            var fieldName = result.Path;
            if (result.MaybeProperty != null &&
                queryGenerator.Conventions.FindIdentityProperty(result.MaybeProperty))
            {
                fieldName = Constants.DocumentIdFieldName;
                fieldType = typeof(string);
            }

            if (queryGenerator.Conventions.UsesRangeType(fieldType))
                fieldName = fieldName + "_Range";
            documentQuery.AddOrder(fieldName, descending, fieldType);
        }

        private bool insideSelect;
        private readonly bool isMapReduce;
        private bool isNotEqualCheckBoundsToAndAlso;
        private Type originalQueryType;

        private void VisitSelect(Expression operand)
        {
            var lambdaExpression = operand as LambdaExpression;
            var body = lambdaExpression != null ? lambdaExpression.Body : operand;
            switch (body.NodeType)
            {
                case ExpressionType.Convert:
                    insideSelect = true;
                    try
                    {
                        VisitSelect(((UnaryExpression)body).Operand);
                    }
                    finally
                    {
                        insideSelect = false;
                    }
                    break;
                case ExpressionType.MemberAccess:
                    var memberExpression = ((MemberExpression)body);
                    AddToFieldsToFetch(GetSelectPath(memberExpression), GetSelectPath(memberExpression));
                    if (insideSelect == false)
                    {
                        foreach (var renamedField in FieldsToRename.Where(x => x.OriginalField == memberExpression.Member.Name).ToArray())
                        {
                            FieldsToRename.Remove(renamedField);
                        }
                        FieldsToRename.Add(new RenamedField
                        {
                            NewField = null,
                            OriginalField = memberExpression.Member.Name
                        });
                    }
                    break;
                    //Anonymous types come through here .Select(x => new { x.Cost } ) doesn't use a member initializer, even though it looks like it does
                    //See http://blogs.msdn.com/b/sreekarc/archive/2007/04/03/immutable-the-new-anonymous-type.aspx
                case ExpressionType.New:
                    var newExpression = ((NewExpression)body);
                    newExpressionType = newExpression.Type;
                    for (int index = 0; index < newExpression.Arguments.Count; index++)
                    {
                        var field = newExpression.Arguments[index] as MemberExpression;
                        if (field == null)
                            continue;
                        var expression = linqPathProvider.GetMemberExpression(newExpression.Arguments[index]);
                        AddToFieldsToFetch(GetSelectPath(expression), GetSelectPath(newExpression.Members[index]));
                    }
                    break;
                    //for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
                case ExpressionType.MemberInit:
                    var memberInitExpression = ((MemberInitExpression)body);
                    newExpressionType = memberInitExpression.NewExpression.Type;
                    foreach (MemberBinding t in memberInitExpression.Bindings)
                    {
                        var field = t as MemberAssignment;
                        if (field == null)
                            continue;

                        var expression = linqPathProvider.GetMemberExpression(field.Expression);
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

        private string GetSelectPath(MemberInfo member)
        {
            return LinqPathProvider.HandlePropertyRenames(member, member.Name);

        }

        private string GetSelectPath(MemberExpression expression)
        {
            var expressionInfo = GetMember(expression);
            return expressionInfo.Path;
        }

        private void AddToFieldsToFetch(string docField, string renamedField)
        {
            var identityProperty = documentQuery.DocumentConvention.GetIdentityProperty(typeof(T));
            if (identityProperty != null && identityProperty.Name == docField)
            {
                FieldsToFetch.Add(Constants.DocumentIdFieldName);
                if (identityProperty.Name != renamedField)
                {
                    docField = Constants.DocumentIdFieldName;
                }
            }
            else
            {
                FieldsToFetch.Add(docField);
            }
            if (renamedField != null && docField != renamedField)
            {
                if (identityProperty == null)
                {
                    var idPropName = documentQuery.DocumentConvention.FindIdentityPropertyNameFromEntityName(documentQuery.DocumentConvention.GetTypeTagName(typeof(T)));
                    if (docField == idPropName)
                    {
                        FieldsToRename.Add(new RenamedField
                        {
                            NewField = renamedField,
                            OriginalField = Constants.DocumentIdFieldName
                        });
                    }
                }
                FieldsToRename.Add(new RenamedField
                {
                    NewField = renamedField,
                    OriginalField = docField
                });
            }
        }

        private void VisitSkip(ConstantExpression constantExpression)
        {
            //Don't have to worry about the cast failing, the Skip() extension method only takes an int
            documentQuery.Skip((int)constantExpression.Value);
        }

        private void VisitTake(ConstantExpression constantExpression)
        {
            //Don't have to worry about the cast failing, the Take() extension method only takes an int
            documentQuery.Take((int)constantExpression.Value);
        }

        private void VisitAll(Expression<Func<T, bool>> predicateExpression)
        {
            throw new NotSupportedException("All() is not supported for linq queries");
        }

        private void VisitAny()
        {
            documentQuery.Take(1);
            queryType = SpecialQueryType.Any;
        }

        private void VisitCount()
        {
            documentQuery.Take(0);
            queryType = SpecialQueryType.Count;
        }

        private void VisitLongCount()
        {
            documentQuery.Take(0);
            queryType = SpecialQueryType.LongCount;
        }

        private void VisitSingle()
        {
            documentQuery.Take(2);
            queryType = SpecialQueryType.Single;
        }

        private void VisitSingleOrDefault()
        {
            documentQuery.Take(2);
            queryType = SpecialQueryType.SingleOrDefault;
        }

        private void VisitFirst()
        {
            documentQuery.Take(1);
            queryType = SpecialQueryType.First;
        }

        private void VisitFirstOrDefault()
        {
            documentQuery.Take(1);
            queryType = SpecialQueryType.FirstOrDefault;
        }

        private string GetFieldNameForRangeQuery(ExpressionInfo expression, object value)
        {
            var identityProperty = documentQuery.DocumentConvention.GetIdentityProperty(typeof(T));
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
                return Constants.DocumentIdFieldName;
            }
            if (documentQuery.DocumentConvention.UsesRangeType(value) && !expression.Path.EndsWith("_Range"))
                return expression.Path + "_Range";
            return expression.Path;
        }


        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        [Obsolete("Use GetDocumentQueryFor instead.")]
        public IDocumentQuery<T> GetLuceneQueryFor(Expression expression)
        {
            return GetDocumentQueryFor(expression);
        }

        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        public IDocumentQuery<T> GetDocumentQueryFor(Expression expression)
        {
            var q = queryGenerator.Query<T>(indexName, isMapReduce);
            q.SetTransformerParameters(transformerParameters);

            documentQuery = (IAbstractDocumentQuery<T>)q;
            documentQuery.SetOriginalQueryType(originalQueryType);
            documentQuery.SetResultTransformer(resultsTransformer);
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
            
            if (customizeQuery != null)
                customizeQuery((IDocumentQueryCustomization)documentQuery);

            return q.SelectFields<T>(FieldsToFetch.ToArray());
        }
        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        public IAsyncDocumentQuery<T> GetDocumentQueryForAsync(Expression expression)
        {            
            var q = queryGenerator.AsyncQuery<T>(indexName, isMapReduce);

            documentQuery = (IAbstractDocumentQuery<T>)q;
            documentQuery.SetOriginalQueryType(originalQueryType);
            documentQuery.SetResultTransformer(resultsTransformer);
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

            if (customizeQuery != null)
                customizeQuery((IDocumentQueryCustomization)documentQuery);
            
            return q.SelectFields<T>(FieldsToFetch.ToArray());
        }
        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        [Obsolete("Use GetAsyncDocumentQueryFor instead")]
        public IAsyncDocumentQuery<T> GetAsyncLuceneQueryFor(Expression expression)
        {
            return GetAsyncDocumentQueryFor(expression);
        }

        /// <summary>
        /// Gets the lucene query.
        /// </summary>
        /// <value>The lucene query.</value>
        public IAsyncDocumentQuery<T> GetAsyncDocumentQueryFor(Expression expression)
        {
            var asyncDocumentQuery = queryGenerator.AsyncQuery<T>(indexName, isMapReduce);
            asyncDocumentQuery.SetResultTransformer(resultsTransformer);
            asyncDocumentQuery.SetTransformerParameters(transformerParameters);
            documentQuery = (IAbstractDocumentQuery<T>)asyncDocumentQuery;
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


            if (customizeQuery != null)
                customizeQuery((IDocumentQueryCustomization)asyncDocumentQuery);

            return asyncDocumentQuery.SelectFields<T>(FieldsToFetch.ToArray());
        }

        /// <summary>
        /// Executes the specified expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public object Execute(Expression expression)
        {
            chainedWhere = false;

            documentQuery = (IAbstractDocumentQuery<T>)GetDocumentQueryFor(expression);
            if (newExpressionType == typeof(T))
                return ExecuteQuery<T>();

            var genericExecuteQuery = typeof(RavenQueryProviderProcessor<T>).GetMethod("ExecuteQuery", BindingFlags.Instance | BindingFlags.NonPublic);
            var executeQueryWithProjectionType = genericExecuteQuery.MakeGenericMethod(newExpressionType);
            return executeQueryWithProjectionType.Invoke(this, new object[0]);
        }

        private object ExecuteQuery<TProjection>()
        {
            var renamedFields = FieldsToFetch.Select(field =>
            {
                var value = FieldsToRename.FirstOrDefault(x => x.OriginalField == field);
                if (value != null)
                    return value.NewField ?? field;
                return field;
            }).ToArray();

            var finalQuery = ((IDocumentQuery<T>)documentQuery).SelectFields<TProjection>(FieldsToFetch.ToArray(), renamedFields);

            //no reason to override a value that may or may not exist there
            if(!String.IsNullOrEmpty(resultsTransformer))
            finalQuery.SetResultTransformer(resultsTransformer);
            finalQuery.SetTransformerParameters(transformerParameters);

            if (FieldsToRename.Count > 0)
                finalQuery.AfterQueryExecuted(RenameResults);
            var executeQuery = GetQueryResult(finalQuery);

            var queryResult = finalQuery.QueryResult;
            if (afterQueryExecuted != null)
            {
                afterQueryExecuted(queryResult);
            }
            return executeQuery;
        }

        public void RenameResults(QueryResult queryResult)
        {
            for (int index = 0; index < queryResult.Results.Count; index++)
            {
                var result = queryResult.Results[index];
                var safeToModify = (RavenJObject)result.CreateSnapshot();

                if (!RenameSingleResult(ref safeToModify))
                    continue;
                safeToModify.EnsureCannotBeChangeAndEnableSnapshotting();
                queryResult.Results[index] = safeToModify;
            }
        }

        public bool RenameSingleResult(ref RavenJObject doc)
        {
            var changed = false;
            var values = new Dictionary<string, RavenJToken>();
            foreach (var renamedField in FieldsToRename.Select(x => x.OriginalField).Distinct())
            {
                RavenJToken value;
                if (doc.TryGetValue(renamedField, out value) == false)
                    continue;
                values[renamedField] = value;
                if (FieldsToFetch.Contains(renamedField) == false)
                {
                    doc.Remove(renamedField);	
                }
            }
            foreach (var rename in FieldsToRename)
            {
                RavenJToken val;
                if (values.TryGetValue(rename.OriginalField, out val) == false)
                    continue;
                changed = true;
                var ravenJObject = val as RavenJObject;
                if (rename.NewField == null && ravenJObject != null)
                {
                    doc = ravenJObject;
                }
                else if (rename.NewField != null)
                {
                    doc[rename.NewField] = val;
                }
                else
                {
                    doc[rename.OriginalField] = val;
                }
            }
            return changed;
        }

        private object GetQueryResult<TProjection>(IDocumentQuery<TProjection> finalQuery)
        {
            switch (queryType)
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
                    if (queryType != SpecialQueryType.Count) 
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
            SingleOrDefault,
        }

#endregion
    }

    public class RenamedField
    {
        public string OriginalField { get; set; }
        public string NewField { get; set; }
    }
}
