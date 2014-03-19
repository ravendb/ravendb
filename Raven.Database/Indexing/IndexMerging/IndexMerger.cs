// -----------------------------------------------------------------------
//  <copyright file="IndexMerger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

namespace Raven.Database.Indexing.IndexMerging
{
    public class IndexMerger
    {
        private readonly Dictionary<int, IndexDefinition> indexDefinitions;

        public IndexMerger(Dictionary<int, IndexDefinition> indexDefinitions)
        {
            this.indexDefinitions = indexDefinitions;
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
            if (indexData.Index.IsMapReduce)
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
            var parser = new CSharpParser();
            var indexes = new List<IndexData>();

            foreach (var kvp in indexDefinitions)
            {
                var index = kvp.Value;
                var indexData = new IndexData(index)
                {
                    IndexId = index.IndexId,
                    IndexName = index.Name,
                    OriginalMap = index.Map,
                };

                indexes.Add(indexData);

                if (index.IsMapReduce || index.Maps.Count > 1)
                {
                    continue;
                }

                Expression map = parser.ParseExpression(index.Map);
                var visitor = new IndexVisitor(indexData);
                map.AcceptVisitor(visitor);
            }
            return indexes;
        }

        private bool CanMergeIndexes(IndexData other, IndexData current)
        {
            if (current.IndexId == other.IndexId)
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

            var currentFromExpression = current.FromExpression as MemberReferenceExpression;
            var otherFromExpression = other.FromExpression as MemberReferenceExpression;

            if (currentFromExpression != null || otherFromExpression != null)
            {
                if (currentFromExpression == null || otherFromExpression == null)
                    return false;

                if (currentFromExpression.MemberName != otherFromExpression.MemberName)
                    return false;
            }

            return CompareIndexFieldOptions(other, current);
        }

        private bool CompareIndexFieldOptions(IndexData index1Data, IndexData index2Data)
        {
            string[] intersectNames = index2Data.SelectExpressions.Keys.Intersect(index1Data.SelectExpressions.Keys).ToArray();

            if (DataDictionaryCompare(index1Data.Stores, index2Data.Stores, intersectNames) == false)
                return false;
            if (DataDictionaryCompare(index1Data.Analyzers, index2Data.Analyzers, intersectNames) == false)
                return false;
            if (DataDictionaryCompare(index1Data.Suggestions, index2Data.Suggestions, intersectNames) == false)
                return false;
            if (DataDictionaryCompare(index1Data.SortOptions, index2Data.SortOptions, intersectNames) == false)
                return false;
            if (DataDictionaryCompare(index1Data.Indexes, index2Data.Indexes, intersectNames) == false)
                return false;
            if (DataDictionaryCompare(index1Data.TermVectors, index2Data.TermVectors, intersectNames) == false)
                return false;
            if (DataDictionaryCompare(index1Data.SpatialIndexes, index2Data.SpatialIndexes, intersectNames) == false)
                return false;


            return true;
        }


        private bool IsDefaultValue(FieldStorage val)
        {
            return val == FieldStorage.No;
        }

        private bool IsDefaultValue(SortOptions val)
        {
            return val == SortOptions.None;
        }

        private bool IsDefaultValue(FieldTermVector val)
        {
            return val == FieldTermVector.No;
        }

        private bool IsDefaultValue(FieldIndexing val)
        {
            return val == FieldIndexing.Default;
        }

        private bool IsDefaultValue(string val)
        {
            return val.Equals(string.Empty);
        }

        private bool IsDefaultValue(SuggestionOptions val)
        {
            var defaultSuggestionOptions = new SuggestionOptions();
            defaultSuggestionOptions.Distance = StringDistanceTypes.None;

            return val.Equals(defaultSuggestionOptions);
        }

        private bool IsDefaultValue<T>(T val)
        {
            Type type = typeof(T);
            var valAsString = val as string;
            if (valAsString != null)
                return IsDefaultValue(valAsString);

            var valAsSuggestion = val as SuggestionOptions;
            if (valAsSuggestion != null)
                return IsDefaultValue(valAsSuggestion);

            if (type.IsEnum)
            {
                if (type.FullName.Equals(typeof(SortOptions).FullName))
                {
                    var valAsSortOption = (SortOptions)Convert.ChangeType(val, typeof(SortOptions));
                    return IsDefaultValue(valAsSortOption);
                }
                if (type.FullName.Equals(typeof(FieldStorage).FullName))
                {
                    var valAsStorage = (FieldStorage)Convert.ChangeType(val, typeof(FieldStorage));
                    return IsDefaultValue(valAsStorage);
                }
                if (type.FullName.Equals(typeof(FieldTermVector).FullName))
                {
                    var valAsTermVector = (FieldTermVector)Convert.ChangeType(val, typeof(FieldTermVector));
                    return IsDefaultValue(valAsTermVector);
                }

                if (type.FullName.Equals(typeof(FieldIndexing).FullName))
                {
                    var valAsIndexing = (FieldIndexing)Convert.ChangeType(val, typeof(FieldIndexing));
                    return IsDefaultValue(valAsIndexing);
                }
            }
            return true;
        }

        private bool DataDictionaryCompare<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2, IEnumerable<string> names)
        {
            bool found1, found2;


            foreach (string kvp in names)
            {
                T v1, v2;
                found1 = dataDict1.TryGetValue(kvp, out v1);
                found2 = dataDict2.TryGetValue(kvp, out v2);


                if (found1 && found2 && Equals(v1, v2) == false)
                    return false;

                // exists only in 1 - check if contains default value
                if (found1 && !found2)
                {
                    if (!IsDefaultValue(v1))
                        return false;
                }
                if (found2 && !found1)
                {
                    if (!IsDefaultValue(v2))
                        return false;
                }
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
                Expression expressionValue;
                if (y.SelectExpressions.TryGetValue(pair.Key, out expressionValue) == false)
                    continue;
                // for the same key, they have to be the same
                string ySelectExpr = expressionValue.ToString();
                string xSelectExpr = pair.Value.ToString();
                if (xSelectExpr != ySelectExpr)
                {
                    return false;
                }
            }
            return true;
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
       
                var selectExpressionDict = new Dictionary<string, Expression>();

                foreach (var curProposedData in mergeProposal.ProposedForMerge)
                {
                    foreach (var curExpr in curProposedData.SelectExpressions)
                    {
                        selectExpressionDict[curExpr.Key] = curExpr.Value;
                    }
                    mergeSuggestion.CanMerge.Add(curProposedData.IndexName);

                    DataDictionaryMerge(mergeSuggestion.MergedIndex.Stores, curProposedData.Stores);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.Indexes, curProposedData.Indexes);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.Analyzers, curProposedData.Analyzers);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.SortOptions, curProposedData.SortOptions);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.Suggestions, curProposedData.Suggestions);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.TermVectors, curProposedData.TermVectors);
                    DataDictionaryMerge(mergeSuggestion.MergedIndex.SpatialIndexes, curProposedData.SpatialIndexes);
                }

                mergeSuggestion.MergedIndex.Map = mergeProposal.ProposedForMerge[0].BuildExpression(selectExpressionDict);

                if (mergeProposal.ProposedForMerge.Count > 1)
                {
                    indexMergeResults.Suggestions.Add(mergeSuggestion);
                }
                if ((mergeProposal.ProposedForMerge.Count == 1) && (mergeProposal.ProposedForMerge[0].IsSuitedForMerge == false))
                {
                    const string comment = "Can't find any other index to merge this with";
                    indexMergeResults.Unmergables.Add(mergeProposal.ProposedForMerge[0].IndexName, comment);
                }
            }
            indexMergeResults = ExcludePartialResults(indexMergeResults);
            return indexMergeResults;
        }


        private IndexMergeResults ExcludePartialResults(IndexMergeResults originalIndexes)
        {
            var resultingIndexMerge = new IndexMergeResults();

            foreach (var suggestion in originalIndexes.Suggestions)
            {
                suggestion.CanMerge.Sort();
            }

            bool hasMatch = false;
            for (int i = 0; i < originalIndexes.Suggestions.Count; i++)
            {
                var sug1 = originalIndexes.Suggestions[i];
                for (int j = i + 1; j < originalIndexes.Suggestions.Count; j++)
                {
                    var sug2 = originalIndexes.Suggestions[j];
                    if ((sug1 != sug2) && (sug1.CanMerge.Count <= sug2.CanMerge.Count))
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

        public IndexMergeResults ProposeIndexMergeSuggestions()
        {
            List<IndexData> indexes = ParseIndexesAndGetReadyToMerge();
            List<MergeProposal> mergedIndexesData = MergeIndexes(indexes);
            IndexMergeResults mergedResults = CreateMergeIndexDefinition(mergedIndexesData);
            return mergedResults;
        }
    }
}