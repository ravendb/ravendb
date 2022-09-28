using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    public class IndexMerger
    {
        private readonly Dictionary<string, IndexDefinition> _indexDefinitions;

        public IndexMerger(Dictionary<string, IndexDefinition> indexDefinitions)
        {
            _indexDefinitions = indexDefinitions;
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
                    indexData.IsSuitedForMerge = false;
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

                    current.IsSuitedForMerge = true;
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
                failComments.Add("Cannot merge map/reduce indexes");
            }

            if (indexData.Index.Maps.Count > 1)
            {
                failComments.Add("Cannot merge multi map indexes");
            }

            if (indexData.NumberOfFromClauses > 1)
            {
                failComments.Add("Cannot merge indexes that have more than a single from clause");
            }

            if (indexData.NumberOfSelectClauses > 1)
            {
                failComments.Add("Cannot merge indexes that have more than a single select clause");
            }

            if (indexData.HasWhere)
            {
                failComments.Add("Cannot merge indexes that have a where clause");
            }

            if (indexData.HasGroup)
            {
                failComments.Add("Cannot merge indexes that have a group by clause");
            }

            if (indexData.HasLet)
            {
                failComments.Add("Cannot merge indexes that are using a let clause");
            }

            if (indexData.HasOrder)
            {
                failComments.Add("Cannot merge indexes that have an order by clause");
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
                visitor.Visit(map);
            }

            return indexes;
        }

        private bool CanMergeIndexes(IndexData other, IndexData current)
        {
            if (current.IndexName == other.IndexName)
                return false;

            if (current.NumberOfFromClauses > 1)
                return false;

            if (current.NumberOfSelectClauses > 1)
                return false;

            if (current.HasWhere)
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
                if (xSelectExpr != ySelectExpr)
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
                _ => ExtractValueFromExpression(expr)
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

                if (TryMergeSelectExpressionsAndFields(mergeProposal, selectExpressionDict, mergeSuggestion, out var mergingComment) == false)
                {
                    indexMergeResults.Unmergables.Add(mergeProposal.MergedData.IndexName, mergingComment);
                    continue;
                }

                TrySetCollectionName(mergeProposal, mergeSuggestion);

                var map = mergeProposal.ProposedForMerge[0].BuildExpression(selectExpressionDict);
                mergeSuggestion.MergedIndex.Maps.Add(map);
                RemoveMatchingIndexes(mergeProposal, selectExpressionDict, mergeSuggestion, indexMergeResults);

                if (mergeProposal.ProposedForMerge.Count == 1 && mergeProposal.ProposedForMerge[0].IsSuitedForMerge == false)
                {
                    const string comment = "Can't find any other index to merge this with";
                    indexMergeResults.Unmergables.Add(mergeProposal.ProposedForMerge[0].IndexName, comment);
                }
            }

            indexMergeResults = ExcludePartialResults(indexMergeResults);
            return indexMergeResults;
        }

        private static void RemoveMatchingIndexes(MergeProposal mergeProposal, Dictionary<string, ExpressionSyntax> selectExpressionDict,
            MergeSuggestions mergeSuggestion,
            IndexMergeResults indexMergeResults)
        {
            if (mergeProposal.ProposedForMerge.Count > 1)
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

        private static void TrySetCollectionName(MergeProposal mergeProposal, MergeSuggestions mergeSuggestion)
        {
            if (mergeProposal.ProposedForMerge[0].Collection != null)
            {
                mergeSuggestion.Collection = mergeProposal.ProposedForMerge[0].Collection;
            }

            else if (mergeProposal.ProposedForMerge[0].FromExpression is SimpleNameSyntax name)
            {
                mergeSuggestion.Collection = name.Identifier.ValueText;
            }

            else if (mergeProposal.ProposedForMerge[0].FromExpression is MemberAccessExpressionSyntax member)
            {
                var identifier = ExtractIdentifierFromExpression(member);
                if (identifier == "docs")
                    mergeSuggestion.Collection = ExtractValueFromExpression(member);
            }
        }

        private static bool TryMergeSelectExpressionsAndFields(MergeProposal mergeProposal, Dictionary<string, ExpressionSyntax> selectExpressionDict,
            MergeSuggestions mergeSuggestion, out string message)
        {
            message = null;
            foreach (var curProposedData in mergeProposal.ProposedForMerge)
            {
                foreach (var curExpr in curProposedData.SelectExpressions)
                {
                    var expression = curExpr.Value as MemberAccessExpressionSyntax;
                    var identifierName = ExtractIdentifierFromExpression(expression);

                    if (identifierName != null && identifierName == curProposedData.FromIdentifier)
                    {
                        expression = ChangeParentInMemberSyntaxToDoc(expression);
                        selectExpressionDict[curExpr.Key] = expression ?? curExpr.Value;
                    }
                    else if (expression is null && curExpr.Value is InvocationExpressionSyntax ies)
                    {
                        selectExpressionDict[curExpr.Key] = RecursivelyTransformInvocationExpressionSyntax(curProposedData, ies, out message);
                        if (message != null)
                            return false;
                    }
                    else
                    {
                        selectExpressionDict[curExpr.Key] = curExpr.Value;
                    }
                }

                mergeSuggestion.CanMerge.Add(curProposedData.IndexName);
                DataDictionaryMerge(mergeSuggestion.MergedIndex.Fields, curProposedData.Index.Fields);
            }

            return true;
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
                ExpressionSyntax result = argument.Expression switch
                {
                    MemberAccessExpressionSyntax maes => ChangeParentInMemberSyntaxToDoc(maes),
                    InvocationExpressionSyntax iesInner => RecursivelyTransformInvocationExpressionSyntax(curProposedData, iesInner, out message),
                    SimpleLambdaExpressionSyntax => argument.Expression,
                    IdentifierNameSyntax ins => ChangeIdentifierToIndexMergerDefaultWhenNeeded(ins),
                    _ => null
                };

                if (result == null)
                {
                    message = $"Currently, {nameof(IndexMerger)} doesn't handle {argument.Expression.GetType()}.";
                    return null;
                }

                rewrittenArguments.Add(SyntaxFactory.Argument(result));
            }

            ExpressionSyntax invocationExpression = ChangeParentInMemberSyntaxToDoc(ies.Expression as MemberAccessExpressionSyntax) ?? ies.Expression;

            return SyntaxFactory.InvocationExpression(invocationExpression,
                SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList<ArgumentSyntax>(rewrittenArguments)));
            
            IdentifierNameSyntax ChangeIdentifierToIndexMergerDefaultWhenNeeded(IdentifierNameSyntax original)
            {
                if (original.ToFullString() == curProposedData.FromIdentifier)
                    return SyntaxFactory.IdentifierName("doc");

                return original;
            }
        }
        
        private static MemberAccessExpressionSyntax ChangeParentInMemberSyntaxToDoc(MemberAccessExpressionSyntax memberAccessExpression)
        {
            if (memberAccessExpression?.Expression is MemberAccessExpressionSyntax)
            {
                var valueStr = ExtractValueFromExpression(memberAccessExpression);
                var valueExp = SyntaxFactory.ParseExpression(valueStr).NormalizeWhitespace();
                var innerName = ExtractIdentifierFromExpression(valueExp as MemberAccessExpressionSyntax);
                var innerMember = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("doc"), SyntaxFactory.IdentifierName(innerName));
                return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, innerMember, memberAccessExpression.Name);
            }

            if (memberAccessExpression?.Expression is SimpleNameSyntax)
            {
                return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, SyntaxFactory.IdentifierName("doc"),
                    memberAccessExpression.Name);
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
            return resultingIndexMerge;
        }

        private static string ExtractValueFromExpression(ExpressionSyntax expression)
        {
            if (expression == null)
                return null;

            var memberExpression = expression as MemberAccessExpressionSyntax;
            if (memberExpression == null)
                return expression.ToString();
            
            var identifier = ExtractIdentifierFromExpression(memberExpression);
            var value = expression.ToString();

            if (identifier == null)
                return value;
            var parts = value.Split('.');
            return parts[0] == identifier ? value.Substring(identifier.Length + 1) : value;
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
