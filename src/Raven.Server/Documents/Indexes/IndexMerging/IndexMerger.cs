using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    public class IndexMerger
    {
        private readonly Dictionary<string, IndexDefinition> _indexDefinitions;
        private static readonly IdentifierNameSyntax DefaultDocumentIdentifier = SyntaxFactory.IdentifierName("doc");
        
        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }
        
        public class TestingStuff
        {
            internal List<string> IndexNamesToThrowOn { get; set; }
            internal Action<List<string>, string> OnTryMergeSelectExpressionsAndFields { get; set; }
        }
        
        private TestingStuff _forTestingPurposes;
        
        public IndexMerger(Dictionary<string, IndexDefinition> indexDefinitions)
        {
            _indexDefinitions = indexDefinitions
                .Where(i => i.Value.Type.IsAuto() == false && i.Value.Type.IsJavaScript() == false)
                .ToDictionary(i => i.Key, i=> i.Value);
        }

        private List<MergeProposal> MergeIndexes(List<IndexData> indexes)
        {
            var mergedIndexesData = new List<MergeProposal>();
            foreach (var indexData in indexes.Where(indexData => !indexData.IsAlreadyMerged))
            {
                indexData.IsAlreadyMerged = true;
                var mergeData = new MergeProposal();

                List<string> failComments = CheckForUnsuitableIndexForMerging(indexData);

                if (failComments.Count != 0)
                {
                    indexData.Comment = string.Join(Environment.NewLine, failComments);
                    mergeData.MergedData = indexData;
                    mergedIndexesData.Add(mergeData);
                    continue;
                }

                mergeData.ProposedForMerge.Add(indexData);

                foreach (IndexData current in indexes) // Note, we have O(N**2) here, known and understood
                {
                    if (current.IsMapReduceOrMultiMap)
                        continue;
                    if (mergeData.ProposedForMerge.All(other => CanMergeIndexes(other, current)) == false)
                        continue;

                    if (AreSelectClausesCompatible(current, indexData) == false)
                        continue;

                    mergeData.ProposedForMerge.Add(current);
                }

                mergedIndexesData.Add(mergeData);
            }

            return mergedIndexesData;
        }

        private static List<string> CheckForUnsuitableIndexForMerging(IndexData indexData)
        {
            var failComments = new List<string>();
            if (indexData.Index.Type == IndexType.MapReduce)
            {
                failComments.Add("Cannot merge map/reduce indexes.");
            }

            if (indexData.Index.Maps.Count > 1)
            {
                failComments.Add("Cannot merge multi map indexes.");
            }

            if (indexData.NumberOfFromClauses > 1)
            {
                failComments.Add("Cannot merge indexes that have more than a single from clause.");
            }

            if (indexData.NumberOfSelectClauses > 1)
            {
                failComments.Add("Cannot merge indexes that have more than a single select clause.");
            }

            if (indexData.HasWhere)
            {
                failComments.Add("Cannot merge indexes that have a where clause.");
            }

            if (indexData.HasGroup)
            {
                failComments.Add("Cannot merge indexes that have a group by clause.");
            }

            if (indexData.HasLet)
            {
                failComments.Add("Cannot merge indexes that are using a let clause.");
            }

            if (indexData.HasOrder)
            {
                failComments.Add("Cannot merge indexes that have an order by clause.");
            }

            if (indexData.IsFanout)
            {
                failComments.Add("Cannot merge fanout indexes.");
            }            
            
            return failComments;
        }

        private List<IndexData> ParseIndexesAndGetReadyToMerge()
        {
            var indexes = new List<IndexData>();

            foreach (var kvp in _indexDefinitions)
            {
                var index = kvp.Value;
                var indexData = new IndexData(index) {IndexName = index.Name, OriginalMaps = index.Maps};

                indexes.Add(indexData);

                if (index.Type == IndexType.MapReduce || index.Maps.Count > 1)
                {
                    indexData.IsMapReduceOrMultiMap = true;
                    continue;
                }

                var map = SyntaxFactory.ParseExpression(indexData.OriginalMaps.FirstOrDefault()).NormalizeWhitespace();
                var visitor = new IndexVisitor(indexData);
                CollectionNameRetriever collectionRetriever = map is QueryExpressionSyntax ? CollectionNameRetriever.QuerySyntax : CollectionNameRetriever.MethodSyntax;
                visitor.Visit(map);
                collectionRetriever.Visit(map);

                if (collectionRetriever.CollectionNames is null)
                {
                    indexData.Collections = new[] {Raven.Client.Constants.Documents.Collections.AllDocumentsCollection};
                }
                else
                {
                    indexData.IsMapReduceOrMultiMap |= collectionRetriever.CollectionNames.Length > 1;
                    indexData.Collections = collectionRetriever.CollectionNames;
                }
            }

            return indexes;
        }

        private bool CanMergeIndexes(IndexData other, IndexData current)
        {
            if (current.Collections.Length > 1 || other.Collections.Length > 1)
                return false;
            
            if (current.Collections[0] != other.Collections[0])
                return false;
            
            if (current.IndexName == other.IndexName)
                return false;

            if (current.NumberOfFromClauses > 1)
                return false;

            if (current.NumberOfSelectClauses > 1)
                return false;

            if (current.HasWhere)
                return false;

            if (current.IsFanout)
                return false;
            
            if (current.HasGroup)
                return false;
            
            if (current.HasOrder)
                return false;
            
            if (current.HasLet)
                return false;

            var currentFromExpression = current.FromExpression as MemberAccessExpressionSyntax;
            var otherFromExpression = other.FromExpression as MemberAccessExpressionSyntax;

            if (currentFromExpression != null || otherFromExpression != null)
            {
                if (currentFromExpression == null || otherFromExpression == null)
                    return false;

                if (currentFromExpression.Name.Identifier.ValueText != otherFromExpression.Name.Identifier.ValueText)
                    return false;
            }

            return CompareIndexFieldOptions(other, current);
        }

        private bool CompareIndexFieldOptions(IndexData index1Data, IndexData index2Data)
        {
            var intersectNames = index2Data.SelectExpressions.Keys.Intersect(index1Data.SelectExpressions.Keys).ToArray();
            return DataDictionaryCompare(index1Data.Index.Fields, index2Data.Index.Fields, intersectNames);
        }

        private static bool DataDictionaryCompare<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2, IEnumerable<string> names)
        {
            bool found1, found2;

            foreach (string kvp in names)
            {
                found1 = dataDict1.TryGetValue(kvp, out T v1);
                found2 = dataDict2.TryGetValue(kvp, out T v2);

                if (found1 && found2 && Equals(v1, v2) == false)
                    return false;

                // exists only in 1 - check if contains default value
                if (found1 && !found2)
                    return false;

                if (found2 && !found1)
                    return false;
            }

            return true;
        }

        private static void DataDictionaryMerge<TKey, TVal>(IDictionary<TKey, TVal> dest, IDictionary<TKey, TVal> src)
        {
            foreach (var val in src)
            {
                dest[val.Key] = val.Value;
            }
        }

        private static bool AreSelectClausesCompatible(IndexData x, IndexData y)
        {
            foreach (var pair in x.SelectExpressions)
            {
                if (y.SelectExpressions.TryGetValue(pair.Key, out ExpressionSyntax expressionValue) == false)
                    continue;
                // for the same key, they have to be the same
                var ySelectExpr = ExtractValueFromExpression(expressionValue);
                var xSelectExpr = ExtractValueFromExpression(pair.Value);
                if (xSelectExpr.Inner != ySelectExpr.Inner)
                {
                    return false;
                }
            }

            return true;
        }

        private static bool AreSelectClausesTheSame(IndexData index, Dictionary<string, ExpressionSyntax> selectExpressionDict)
        {
            // We want to delete an index when that index is a subset of another.
            if (index.SelectExpressions.Count < selectExpressionDict.Count)
                 return false;

            foreach (var pair in index.SelectExpressions)
            {
                if (selectExpressionDict.TryGetValue(pair.Key, out ExpressionSyntax expressionValue) == false)
                    return false;
                
                // for the same key, they have to be the same
                var ySelectExpr = TransformAndExtractValueFromExpression(expressionValue);
                var xSelectExpr = TransformAndExtractValueFromExpression(pair.Value);
                if (xSelectExpr != ySelectExpr)
                {
                    return false;
                }
            }

            return true;

            string TransformAndExtractValueFromExpression(ExpressionSyntax expr) => expr switch
            {
                InvocationExpressionSyntax ies => RecursivelyTransformInvocationExpressionSyntax(index, ies, out var _).ToString(),
                _ => ExtractValueFromExpression(expr).Inner
            };
        }

        private IndexMergeResults CreateMergeIndexDefinition(List<MergeProposal> indexDataForMerge)
        {
            var indexMergeResults = new IndexMergeResults();
            foreach (var mergeProposal in indexDataForMerge.Where(m => m.ProposedForMerge.Count == 0 && m.MergedData != null))
            {
                indexMergeResults.Unmergables.Add(mergeProposal.MergedData.IndexName, mergeProposal.MergedData.Comment);
            }

            foreach (var mergeProposal in indexDataForMerge)
            {
                if (mergeProposal.ProposedForMerge.Count == 0)
                    continue;

                var mergeSuggestion = new MergeSuggestions();
                var selectExpressionDict = new Dictionary<string, ExpressionSyntax>();

                var isMergeSuccessful = TryMergeSelectExpressionsAndFields(mergeProposal, selectExpressionDict, mergeSuggestion, out var errors);

                AddMergeErrors(indexMergeResults, errors);
                
                if (isMergeSuccessful == false)
                    continue;

                var firstMergeableIndexData = mergeProposal.ProposedForMerge.First(x => x.IndexName.In(mergeSuggestion.CanMerge));

                TrySetCollectionName(firstMergeableIndexData, mergeSuggestion);
                
                const string comment = "Can't find any other index to merge this index with.";

                var map = firstMergeableIndexData.BuildExpression(selectExpressionDict);
                if (map is null)
                {
                    indexMergeResults.Unmergables.Add(firstMergeableIndexData.IndexName, comment);
                    continue;
                } 

                mergeSuggestion.MergedIndex.Maps.Add(SourceCodeBeautifier.FormatIndex(map).Expression);
                SuggestIndexesToDelete(mergeProposal, selectExpressionDict, mergeSuggestion, indexMergeResults);

                if (mergeSuggestion.CanMerge.Count == 1)
                    indexMergeResults.Unmergables[firstMergeableIndexData.IndexName] = comment;
            }

            indexMergeResults = ExcludePartialResults(indexMergeResults);
            return indexMergeResults;
        }

        private static void SuggestIndexesToDelete(MergeProposal mergeProposal, Dictionary<string, ExpressionSyntax> selectExpressionDict,
            MergeSuggestions mergeSuggestion,
            IndexMergeResults indexMergeResults)
        {
            if (mergeProposal.ProposedForMerge.Count > 1 && mergeSuggestion.CanMerge.Count > 1)
            {
                var matchingExistingIndexes = mergeProposal.ProposedForMerge.Where(x =>
                        AreSelectClausesTheSame(x, selectExpressionDict) &&
                        (x.Index.Compare(mergeSuggestion.MergedIndex) == IndexDefinitionCompareDifferences.None
                         || x.Index.Compare(mergeSuggestion.MergedIndex) == IndexDefinitionCompareDifferences.Maps))
                    .OrderBy(x => x.IndexName.StartsWith("Auto/", StringComparison.CurrentCultureIgnoreCase))
                    .ToList();

                if (matchingExistingIndexes.Count > 0)
                {
                    var surpassingIndex = matchingExistingIndexes.First();
                    mergeSuggestion.SurpassingIndex = surpassingIndex.IndexName;

                    mergeSuggestion.MergedIndex = null;
                    mergeSuggestion.CanMerge.Clear();
                    mergeSuggestion.CanDelete = mergeProposal.ProposedForMerge.Except(new[] {surpassingIndex}).Select(x => x.IndexName).ToList();
                }

                indexMergeResults.Suggestions.Add(mergeSuggestion);
            }
        }

        private static void AddMergeErrors(IndexMergeResults indexMergeResults, List<MergeError> errors)
        {
            var alreadyAddedIndexNames = indexMergeResults.Errors.Select(x => x.IndexName).ToList();
            
            foreach (var error in errors) 
            {
                if (alreadyAddedIndexNames.Contains(error.IndexName) == false)
                    indexMergeResults.Errors.Add(error);
            }
        }

        private static void TrySetCollectionName(IndexData indexData, MergeSuggestions mergeSuggestion)
        {
            if (indexData.Collections != null)
            {
                mergeSuggestion.Collection = indexData.Collections[0];
            }

            else if (indexData.FromExpression is SimpleNameSyntax name)
            {
                mergeSuggestion.Collection = name.Identifier.ValueText;
            }

            else if (indexData.FromExpression is MemberAccessExpressionSyntax member)
            {
                var identifier = ExtractIdentifierFromExpression(member);
                if (identifier == "docs")
                    mergeSuggestion.Collection = ExtractValueFromExpression(member).Identifier;
            }
        }

        private bool TryMergeSelectExpressionsAndFields(MergeProposal mergeProposal, Dictionary<string, ExpressionSyntax> selectExpressionDict,
            MergeSuggestions mergeSuggestion, out List<MergeError> mergeErrors)
        {
            mergeErrors = new List<MergeError>();
            
            foreach (var curProposedData in mergeProposal.ProposedForMerge)
            {
                try
                {
                    _forTestingPurposes?.OnTryMergeSelectExpressionsAndFields?.Invoke(_forTestingPurposes.IndexNamesToThrowOn, curProposedData.IndexName);
                    
                    foreach (var curExpr in curProposedData.SelectExpressions)
                    {
                        var expr = curExpr.Value;
                        var rewritten = RewriteExpressionSyntax(curProposedData, expr, out var message);
                        selectExpressionDict[curExpr.Key] = rewritten ?? expr;
                    }

                    mergeSuggestion.CanMerge.Add(curProposedData.IndexName);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.Fields, curProposedData.Index.Fields);
                }
                catch (Exception ex)
                {
                    var mergeError = new MergeError() { IndexName = curProposedData.IndexName, Message = ex.Message, StackTrace = ex.StackTrace };
                    mergeErrors.Add(mergeError);
                }
            }

            return mergeSuggestion.CanMerge.Count > 0;
        }

        private static InvocationExpressionSyntax RecursivelyTransformInvocationExpressionSyntax(IndexData curProposedData, InvocationExpressionSyntax ies, out string message)
        {
            message = null;
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            {
                message = "Index is too complex. Cannot apply merging on it.";
                return null;
            }

            List<ArgumentSyntax> rewrittenArguments = new();
            foreach (var argument in ies.ArgumentList.Arguments)
            {
                ExpressionSyntax result = RewriteExpressionSyntax(curProposedData, argument.Expression, out message);

                if (result == null)
                {
                    message = $"Currently, {nameof(IndexMerger)} doesn't handle {argument.Expression.GetType()}.";
                    return null;
                }

                rewrittenArguments.Add(SyntaxFactory.Argument(result));
            }

            ExpressionSyntax invocationExpression = RewriteExpressionSyntax(curProposedData, ies.Expression, out message);

            return SyntaxFactory.InvocationExpression(invocationExpression,
                SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList<ArgumentSyntax>(rewrittenArguments)));
        }

        private static ExpressionSyntax RewriteExpressionSyntax(IndexData indexData, ExpressionSyntax originalExpression, out string message)
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            {
                message = "Index is too complex. Cannot apply merging on it.";
                return null;
            }
            message = null;
            return originalExpression switch
            {
                MemberAccessExpressionSyntax maes => ChangeParentInMemberSyntaxToDoc(indexData, maes),
                InvocationExpressionSyntax iesInner => RecursivelyTransformInvocationExpressionSyntax(indexData, iesInner, out message),
                SimpleLambdaExpressionSyntax =>  originalExpression,
                IdentifierNameSyntax ins => ChangeIdentifierToIndexMergerDefaultWhenNeeded(ins), 
                BinaryExpressionSyntax bes => RewriteBinaryExpression(indexData, bes, out message),
                ParenthesizedExpressionSyntax pes => RewriteParenthesizedExpressionSyntax(indexData, pes, out message),
                LiteralExpressionSyntax => originalExpression,
                ConditionalExpressionSyntax ces => RewriteConditionalExpressionSyntax(indexData, ces, out message),
                PrefixUnaryExpressionSyntax pues => pues,
                CastExpressionSyntax ces => RewriteCastExpressionSyntax(indexData, ces, out message),
                ElementAccessExpressionSyntax eaes => RewriteElementAccessExpressionSyntax(indexData, eaes, out message),
                ConditionalAccessExpressionSyntax caes => caes,
                _ => null
            };
            
            IdentifierNameSyntax ChangeIdentifierToIndexMergerDefaultWhenNeeded(IdentifierNameSyntax original)
            {
                if (original.ToFullString() == indexData.FromIdentifier)
                    return DefaultDocumentIdentifier;

                return original;
            }
        }

        private static ExpressionSyntax RewriteElementAccessExpressionSyntax(IndexData indexData, ElementAccessExpressionSyntax eaes, out string message)
        {
            var innerExpression = RewriteExpressionSyntax(indexData, eaes.Expression, out message);
            return SyntaxFactory.ElementAccessExpression(innerExpression, eaes.ArgumentList);
        }

        private static ExpressionSyntax RewriteCastExpressionSyntax(IndexData indexData, CastExpressionSyntax ces, out string message)
        {
            var innerExpression = RewriteExpressionSyntax(indexData, ces.Expression, out message);
            return SyntaxFactory.CastExpression(ces.OpenParenToken, ces.Type, ces.CloseParenToken, innerExpression);
        }

        private static ExpressionSyntax RewriteParenthesizedExpressionSyntax(IndexData indexData, ParenthesizedExpressionSyntax pes, out string message)
        {
            var innerExpression = pes.Expression;
            var expressionSyntax = RewriteExpressionSyntax(indexData, innerExpression, out message);
            return SyntaxFactory.ParenthesizedExpression(expressionSyntax);
        }

        private static ConditionalExpressionSyntax RewriteConditionalExpressionSyntax(IndexData indexData, ConditionalExpressionSyntax ces, out string message)
        {
            var condition = RewriteExpressionSyntax(indexData, ces.Condition, out message);
            if (message is not null)
                return ces;
            
            var whenTrue = RewriteExpressionSyntax(indexData, ces.WhenTrue, out message);
            if (message is not null)
                return ces;

            var whenFalse = RewriteExpressionSyntax(indexData, ces.WhenFalse, out message);
            if (message is not null)
                return ces;

            return SyntaxFactory.ConditionalExpression(condition, whenTrue, whenFalse);
        }
        
        internal static ExpressionSyntax StripExpressionParenthesis(ExpressionSyntax expr)
        {
            while (expr is ParenthesizedExpressionSyntax)
            {
                expr = ((ParenthesizedExpressionSyntax)expr).Expression;
            }

            return expr;
        }

        internal static SyntaxNode StripExpressionParentParenthesis(SyntaxNode expr)
        {
            if (expr == null)
                return null;
            while (expr.Parent is ParenthesizedExpressionSyntax)
            {
                expr = ((ParenthesizedExpressionSyntax)expr.Parent).Parent;
            }

            return expr.Parent;
        }
        
        private static BinaryExpressionSyntax RewriteBinaryExpression(IndexData indexData, BinaryExpressionSyntax original, out string message)
        {
            var leftSide = RewriteExpressionSyntax(indexData, original.Left, out var m1);
            var rightSide = RewriteExpressionSyntax(indexData, original.Right, out var m2);
            message = m1 is null && m2 is null ? null : $"{m1 ?? string.Empty} | {m2 ?? string.Empty}";
            return SyntaxFactory.BinaryExpression(original.Kind(), leftSide, original.OperatorToken, rightSide);
        }
        
        private static MemberAccessExpressionSyntax ChangeParentInMemberSyntaxToDoc(IndexData data, MemberAccessExpressionSyntax memberAccessExpression)
        {
            if (memberAccessExpression?.Expression is MemberAccessExpressionSyntax child)
            {
                var original = ExtractValueFromExpression(child);
                var rewrittenInner = SyntaxFactory.ParseExpression(original.Inner).NormalizeWhitespace();
                var inner = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression,
                    DefaultDocumentIdentifier, SyntaxFactory.IdentifierName(original.Inner));
                return SyntaxFactory.MemberAccessExpression(memberAccessExpression.Kind(), inner, memberAccessExpression.Name);
            }
            
            if (memberAccessExpression?.Expression is SimpleNameSyntax sns )
            {
                if (sns.Identifier.ToString() != data.FromIdentifier)
                    return memberAccessExpression;
                
                return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, DefaultDocumentIdentifier,
                    memberAccessExpression.Name);
            }
            
            if (memberAccessExpression?.Expression is InvocationExpressionSyntax ies)
            {
                var expr = RecursivelyTransformInvocationExpressionSyntax(data, ies, out var _);
                return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, expr, memberAccessExpression.Name);
            }

            if (memberAccessExpression?.Expression is ThisExpressionSyntax tes)
            {
                return memberAccessExpression;
            }
            
            return null;
        }

        
        private static IndexMergeResults ExcludePartialResults(IndexMergeResults originalIndexes)
        {
            var resultingIndexMerge = new IndexMergeResults();

            foreach (var suggestion in originalIndexes.Suggestions)
            {
                suggestion.CanMerge.Sort();
            }

            var hasMatch = false;
            for (var i = 0; i < originalIndexes.Suggestions.Count; i++)
            {
                var sug1 = originalIndexes.Suggestions[i];
                for (var j = i + 1; j < originalIndexes.Suggestions.Count; j++)
                {
                    var sug2 = originalIndexes.Suggestions[j];
                    if (sug1 != sug2 && sug1.CanMerge.Count <= sug2.CanMerge.Count)
                    {
                        var sugCanMergeSet = new HashSet<string>(sug1.CanMerge);
                        hasMatch = sugCanMergeSet.IsSubsetOf(sug2.CanMerge);
                        if (hasMatch)
                            break;
                    }
                }

                if (!hasMatch)
                {
                    resultingIndexMerge.Suggestions.Add(sug1);
                }

                hasMatch = false;
            }

            resultingIndexMerge.Unmergables = originalIndexes.Unmergables;
            resultingIndexMerge.Errors = originalIndexes.Errors;
            
            return resultingIndexMerge;
        }

        private static (string Identifier, string Inner) ExtractValueFromExpression(ExpressionSyntax expression)
        {
            if (expression == null)
                return (null, null);

            var memberExpression = expression as MemberAccessExpressionSyntax;
            if (memberExpression == null)
                return (null, expression.ToString());
            
            var identifier = ExtractIdentifierFromExpression(memberExpression);
            var value = expression.ToString();

            if (identifier == null)
                return (null, value);
            var parts = value.Split('.');
            return parts[0] == identifier ? (identifier, value.Substring(identifier.Length + 1)) : (null, value);
        }

        private static string ExtractIdentifierFromExpression(MemberAccessExpressionSyntax expression)
        {
            var node = expression?.Expression;
            while (node != null)
            {
                if (!(node is MemberAccessExpressionSyntax))
                    break;

                node = (node as MemberAccessExpressionSyntax).Expression;
            }

            if (node == null)
                return null;

            var identifier = node as IdentifierNameSyntax;
            return identifier?.Identifier.ValueText;
        }

        public IndexMergeResults ProposeIndexMergeSuggestions()
        {
            var indexes = ParseIndexesAndGetReadyToMerge();
            var mergedIndexesData = MergeIndexes(indexes);
            var mergedResults = CreateMergeIndexDefinition(mergedIndexesData);
            return mergedResults;
        }
    }
}
