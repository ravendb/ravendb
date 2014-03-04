//-----------------------------------------------------------------------
// <copyright file="IndexDefinitionStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Lucene.Net.Analysis.Standard;
using Microsoft.Isam.Esent.Interop;
using Mono.CSharp.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Indexing;

namespace Raven.Database.Storage
{
    public class IndexDefinitionStorage
    {
        private const string IndexDefDir = "IndexDefinitions";

        private readonly ReaderWriterLockSlim currentlyIndexingLock = new ReaderWriterLockSlim();
        public long currentlyIndexing;

        private readonly ConcurrentDictionary<int, AbstractViewGenerator> indexCache =
            new ConcurrentDictionary<int, AbstractViewGenerator>();

        private readonly ConcurrentDictionary<int, AbstractTransformer> transformCache =
            new ConcurrentDictionary<int, AbstractTransformer>();

        private readonly ConcurrentDictionary<string, int> indexNameToId =
            new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, int> transformNameToId =
            new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<int, TransformerDefinition> transformDefinitions =
            new ConcurrentDictionary<int, TransformerDefinition>();

        private readonly ConcurrentDictionary<int, IndexDefinition> indexDefinitions =
            new ConcurrentDictionary<int, IndexDefinition>();

        private readonly ConcurrentDictionary<int, IndexDefinition> newDefinitionsThisSession =
            new ConcurrentDictionary<int, IndexDefinition>();

        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        private readonly string path;
        private readonly InMemoryRavenConfiguration configuration;
        private readonly OrderedPartCollection<AbstractDynamicCompilationExtension> extensions;

        private List<IndexMergeSuggestion> IndexMergeSuggestions { get; set; }
        public IndexDefinitionStorage(
            InMemoryRavenConfiguration configuration,
            ITransactionalStorage transactionalStorage,
            string path,
            IEnumerable<AbstractViewGenerator> compiledGenerators,
            OrderedPartCollection<AbstractDynamicCompilationExtension> extensions)
        {
            IndexMergeSuggestions = new List<IndexMergeSuggestion>();
            this.configuration = configuration;
            this.extensions = extensions; // this is used later in the ctor, so it must appears first
            this.path = Path.Combine(path, IndexDefDir);

            if (Directory.Exists(this.path) == false && configuration.RunInMemory == false)
                Directory.CreateDirectory(this.path);

            if (configuration.RunInMemory == false)
                ReadFromDisk();

            newDefinitionsThisSession.Clear();
        }

        public bool IsNewThisSession(IndexDefinition definition)
        {
            return newDefinitionsThisSession.ContainsKey(definition.IndexId);
        }

        private void ReadFromDisk()
        {
            foreach (var index in Directory.GetFiles(path, "*.index"))
            {
                try
                {
                    var indexDefinition = JsonConvert.DeserializeObject<IndexDefinition>(File.ReadAllText(index),
                                                                                         Default.Converters);
                    ResolveAnalyzers(indexDefinition);
                    AddAndCompileIndex(indexDefinition);
                    AddIndex(indexDefinition.IndexId, indexDefinition);
                }
                catch (Exception e)
                {
                    logger.WarnException("Could not compile index " + index + ", skipping bad index", e);
                }
            }

            foreach (var transformer in Directory.GetFiles(path, "*.transform"))
            {
                try
                {
                    var indexDefinition =
                        JsonConvert.DeserializeObject<TransformerDefinition>(File.ReadAllText(transformer),
                                                                             Default.Converters);
                    AddAndCompileTransform(indexDefinition);
                    AddTransform(indexDefinition.IndexId, indexDefinition);
                }
                catch (Exception e)
                {
                    logger.WarnException("Could not compile transformer " + transformer + ", skipping bad transformer",
                                         e);
                }
            }
        }

        public int IndexesCount
        {
            get { return indexCache.Count; }
        }

        public string[] IndexNames
        {
            get { return IndexDefinitions.Values.OrderBy(x => x.Name).Select(x => x.Name).ToArray(); }
        }

        public int[] Indexes
        {
            get { return indexCache.Keys.OrderBy(name => name).ToArray(); }
        }

        public string IndexDefinitionsPath
        {
            get { return path; }
        }

        public string[] TransformerNames
        {
            get
            {
                return transformDefinitions.Values
                                           .OrderBy(x => x.Name)
                                           .Select(x => x.Name)
                                           .ToArray();
            }
        }
        public ConcurrentDictionary<int, IndexDefinition> IndexDefinitions
        {
            get { return indexDefinitions; }
        }


        public string CreateAndPersistIndex(IndexDefinition indexDefinition)
        {
            var transformer = AddAndCompileIndex(indexDefinition);
            if (configuration.RunInMemory == false)
            {
                WriteIndexDefinition(indexDefinition);
            }
            return transformer.Name;
        }

        private void WriteIndexDefinition(IndexDefinition indexDefinition)
        {
            if (configuration.RunInMemory)
                return;
            var indexName = Path.Combine(path, indexDefinition.IndexId + ".index");
            File.WriteAllText(indexName,
                              JsonConvert.SerializeObject(indexDefinition, Formatting.Indented, Default.Converters));
        }

        private void WriteTransformerDefinition(TransformerDefinition transformerDefinition)
        {
            if (configuration.RunInMemory)
                return;
            string indexName;
            int i = 0;
            while (true)
            {
                indexName = Path.Combine(path, i + ".transform");
                if (File.Exists(indexName) == false)
                    break;
                i++;
            }
            File.WriteAllText(indexName,
                              JsonConvert.SerializeObject(transformerDefinition, Formatting.Indented, Default.Converters));
        }

        public string CreateAndPersistTransform(TransformerDefinition transformerDefinition)
        {
            var transformer = AddAndCompileTransform(transformerDefinition);
            if (configuration.RunInMemory == false)
            {
                WriteTransformerDefinition(transformerDefinition);
            }
            return transformer.Name;
        }

        public void UpdateIndexDefinitionWithoutUpdatingCompiledIndex(IndexDefinition definition)
        {
            IndexDefinitions.AddOrUpdate(definition.IndexId, s =>
            {
                throw new InvalidOperationException(
                    "Cannot find index named: " +
                    definition.IndexId);
            }, (s, indexDefinition) => definition);
            WriteIndexDefinition(definition);
        }

        private DynamicViewCompiler AddAndCompileIndex(IndexDefinition indexDefinition)
        {
            var transformer = new DynamicViewCompiler(indexDefinition.Name, indexDefinition, extensions, path,
                                                      configuration);
            var generator = transformer.GenerateInstance();
            indexCache.AddOrUpdate(indexDefinition.IndexId, generator, (s, viewGenerator) => generator);

            logger.Info("New index {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name, transformer.CompiledQueryText,
                        transformer.CompiledQueryText);
            return transformer;
        }

        private DynamicTransformerCompiler AddAndCompileTransform(TransformerDefinition transformerDefinition)
        {
            var transformer = new DynamicTransformerCompiler(transformerDefinition, configuration, extensions,
                                                             transformerDefinition.Name, path);
            var generator = transformer.GenerateInstance();
            transformCache.AddOrUpdate(transformerDefinition.IndexId, generator, (s, viewGenerator) => generator);

            logger.Info("New transformer {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name,
                        transformer.CompiledQueryText,
                        transformer.CompiledQueryText);
            return transformer;
        }

        public void RegisterNewIndexInThisSession(string name, IndexDefinition definition)
        {
            newDefinitionsThisSession.TryAdd(definition.IndexId, definition);
        }

        public void AddIndex(int id, IndexDefinition definition)
        {
            IndexDefinitions.AddOrUpdate(id, definition, (s1, def) =>
            {
                if (def.IsCompiled)
                    throw new InvalidOperationException("Index " + id + " is a compiled index, and cannot be replaced");
                return definition;
            });
            indexNameToId[definition.Name] = id;

            UpdateIndexMappingFile();
        }

        public void AddTransform(int id, TransformerDefinition definition)
        {
            transformDefinitions.AddOrUpdate(id, definition, (s1, def) => definition);
            transformNameToId[definition.Name] = id;

            UpdateTransformerMappingFile();
        }

        public void RemoveIndex(int id)
        {
            AbstractViewGenerator ignoredViewGenerator;
            int ignoredId;
            if (indexCache.TryRemove(id, out ignoredViewGenerator))
                indexNameToId.TryRemove(ignoredViewGenerator.Name, out ignoredId);
            IndexDefinition ignoredIndexDefinition;
            IndexDefinitions.TryRemove(id, out ignoredIndexDefinition);
            newDefinitionsThisSession.TryRemove(id, out ignoredIndexDefinition);
            if (configuration.RunInMemory)
                return;
            File.Delete(GetIndexSourcePath(id) + ".index");
            UpdateIndexMappingFile();
        }

        private string GetIndexSourcePath(int id)
        {
            return Path.Combine(path, id.ToString());
        }

        public IndexDefinition GetIndexDefinition(string name)
        {
            int id = 0;
            if (indexNameToId.TryGetValue(name, out id))
                return IndexDefinitions[id];
            return null;
        }

        public IndexDefinition GetIndexDefinition(int id)
        {
            IndexDefinition value;
            IndexDefinitions.TryGetValue(id, out value);
            return value;
        }

        public TransformerDefinition GetTransformerDefinition(string name)
        {
            int id = 0;
            if (transformNameToId.TryGetValue(name, out id))
                return transformDefinitions[id];
            return null;
        }

        public IndexMergeResults ProposeIndexMergeSuggestions()
        {
           
            var indexMergeResults= CreatePossibleIndexMergeSuggestionsList(); 

            return indexMergeResults;
        }
        public IndexMergeResults CreatePossibleIndexMergeSuggestionsList()
        {
            var parser = new CSharpParser();
            var indexes = new List<QueryData>();


            foreach (var kvp in IndexDefinitions)
            {
                var index = kvp.Value;
                if (index.IsMapReduce)
                    continue;

                if(index.Maps.Count>1)  //no support for multiple maps
                    continue;

                var map = parser.ParseExpression(index.Map); // TODO: Multiple maps

                var visitor = new Regina_PleaseRenameMeSoonest_Visitor();
                map.AcceptVisitor(visitor);  
                var curQuery = new QueryData
                {
                    IndexId = index.IndexId,
                    IndexName = index.Name,
                    OriginalQuery = index.Map, 

                    HasWhere = visitor.HasWhere,
                    FromExpression = visitor.FromExpression,
                    FromIdentifier = visitor.FromIdentifier,
                    NumberOfFromClauses = visitor.NumberOfFromClauses,
                    SelectExpressions = visitor.SelectExpressions,
                };
                indexes.Add(curQuery);
            }

            //merge index data
            var mergedQueries = new List<MergeProposal>();
            foreach (var queryData in indexes)
            {
                var mergeData = new MergeProposal();

                if (queryData.IsAlreadyMerged)
                    continue;

                queryData.IsAlreadyMerged = true;

                List<string> failComments = new List<string>();
                if (queryData.NumberOfFromClauses > 1)
                {
                    failComments.Add("Cannot merge indexes that have more than 1 from clause");
                }
                if (queryData.HasWhere)
                {
                    failComments.Add("Cannot merge indexes that have a where clause");
                }
                if (failComments.Count != 0)
                {
                    queryData.Comment = string.Join(Environment.NewLine, failComments);
                    queryData.IsSuitedForMerge = false;
                    var data = new MergeProposal
                    {
                        MergedData = queryData
                    };
                    mergedQueries.Add(data);
                    continue;
                }
                
                mergeData.ProposedForMerge.Add(queryData);
                foreach (var curQueryData in indexes)
                {
                    if (curQueryData.IndexId == queryData.IndexId)
                        continue;

                    if (curQueryData.NumberOfFromClauses > 1)
                        continue;

                    if (curQueryData.IsAlreadyMerged)
                        continue;

                    if (curQueryData.FromExpression != queryData.FromExpression)
                        continue;

                    if (curQueryData.HasWhere)
                        continue;
//TODO: add compare for additional indexdef properties before merge decision
                    if (CompareSelectExpression(curQueryData, queryData))
                    {
                        curQueryData.IsSuitedForMerge = true;
                        mergeData.ProposedForMerge.Add(curQueryData);

                    }

                }

                var newData = new MergeProposal
                {
                    ProposedForMerge = mergeData.ProposedForMerge,
                    MergedData = mergeData.MergedData
                };
                mergedQueries.Add(newData);



            }
            var mergedResults = MergeIndexData(mergedQueries);
            return mergedResults;

        }
        private bool CompareSelectExpression(QueryData expr1, QueryData expr2)
        {
            Expression ExpressionValue;
            foreach (var pair in expr1.SelectExpressions)
            {
                string pairValueStr = pair.Value.ToString();
                if (expr2.SelectExpressions.TryGetValue(pair.Key, out ExpressionValue)) //for the same key val has to be the same
                {
                    string expressionValueStr = ExpressionValue.ToString();
                    if (!pairValueStr.Equals(expressionValueStr))
                    {

                        var splittedExp1 = pairValueStr.Split('.');  //required to add case where xxx. included and different
                        var splittedExp2 = expressionValueStr.Split('.');
                        if (splittedExp1[splittedExp1.Length - 1] != splittedExp2[splittedExp2.Length - 1])
                            return false;
                    }
                }
            }
            return true;
        }

        private IndexMergeResults MergeIndexData(List<MergeProposal> indexDataForMerge)
        {
            var indexMergeResults = new IndexMergeResults();
            var queryData = new QueryData();
            foreach (var mergeProposal in indexDataForMerge)
            {
                var mergeSuggestion = new MergeSuggestions();
                if (mergeProposal.ProposedForMerge.Count > 1)
                {
                    queryData = mergeProposal.ProposedForMerge.First();
                    mergeSuggestion.MergedIndexName = queryData.IndexName;
                    var suggestion = new StringBuilder();
                    suggestion.Append("From " + queryData.FromIdentifier + " In " + queryData.FromExpression);
                    var selectExpressionDict = new HashSet<string>();
                    string canMergeIndexName=string.Empty;
                    foreach (var curProposedData in mergeProposal.ProposedForMerge)
                    {
                        foreach (var curExpr in curProposedData.SelectExpressions)
                        {
                            var valueStr = curExpr.Value.ToString();
                            if (curProposedData.FromIdentifier != queryData.FromIdentifier)
                            {
                                valueStr = valueStr.Replace(curProposedData.FromIdentifier, queryData.FromIdentifier);

                            }
                            var resultValue = valueStr.Split('=');
                            selectExpressionDict.Add(resultValue[resultValue.Length - 1].Trim());
                        }
                        if (!string.Equals(queryData.IndexName, curProposedData.IndexName))
                        {
                            if(!string.IsNullOrEmpty(canMergeIndexName))
                                 canMergeIndexName = canMergeIndexName + " " + curProposedData.IndexName;
                            else
                            {
                                canMergeIndexName = curProposedData.IndexName;
                            }   
                        }
                    }
                    var sb = new StringBuilder();
                    sb.Append("select new { ");
                    foreach (var value in selectExpressionDict)
                    {
                        sb.Append(value);
                        sb.Append(" ,");
                    }
                    suggestion.Append(" " + sb);
                   
                    int len = suggestion.ToString().Length;
                    suggestion.Replace(",", "}", len - 2, 2);
                    if (!mergeSuggestion.CanMergeWith.ContainsKey(canMergeIndexName))
                        mergeSuggestion.CanMergeWith.Add(canMergeIndexName,suggestion.ToString());
                }
                else
                {
                    if (mergeProposal.MergedData != null)
                        indexMergeResults.Unmergables.Add(mergeProposal.MergedData.IndexName,
                            mergeProposal.MergedData.Comment);
                }
                if (mergeProposal.ProposedForMerge.Count > 1)
                {
                    if (!indexMergeResults.Suggestions.ContainsKey(mergeSuggestion.MergedIndexName))
                        indexMergeResults.Suggestions.Add(mergeSuggestion.MergedIndexName, mergeSuggestion);
                }
            }
            return indexMergeResults;
        }

       

      
        public TransformerDefinition GetTransformerDefinition(int id)
        {
            TransformerDefinition value;
            transformDefinitions.TryGetValue(id, out value);
            return value;
        }

        public AbstractViewGenerator GetViewGenerator(string name)
        {
            int id = 0;
            if (indexNameToId.TryGetValue(name, out id))
                return indexCache[id];
            return null;
        }

        public AbstractViewGenerator GetViewGenerator(int id)
        {
            AbstractViewGenerator value;
            if (indexCache.TryGetValue(id, out value) == false)
                return null;
            return value;
        }

        public IndexCreationOptions FindIndexCreationOptions(IndexDefinition indexDef)
        {
            var indexDefinition = GetIndexDefinition(indexDef.Name);
            if (indexDefinition != null)
            {
                indexDef.IndexId = indexDefinition.IndexId;
                bool result = indexDefinition.Equals(indexDef);
                return result
                           ? IndexCreationOptions.Noop
                           : IndexCreationOptions.Update;
            }
            return IndexCreationOptions.Create;
        }

        public bool Contains(string indexName)
        {
            return indexNameToId.ContainsKey(indexName);
        }

        public static void ResolveAnalyzers(IndexDefinition indexDefinition)
        {
            // Stick Lucene.Net's namespace to all analyzer aliases that are missing a namespace
            var analyzerNames = (from analyzer in indexDefinition.Analyzers
                                 where analyzer.Value.IndexOf('.') == -1
                                 select analyzer).ToArray();

            // Only do this for analyzer that actually exist; we do this here to be able to throw a correct error later on
            foreach (
                var a in
                    analyzerNames.Where(
                        a => typeof(StandardAnalyzer).Assembly.GetType("Lucene.Net.Analysis." + a.Value) != null))
            {
                indexDefinition.Analyzers[a.Key] = "Lucene.Net.Analysis." + a.Value;
            }
        }

        public IDisposable TryRemoveIndexContext()
        {
            if (currentlyIndexingLock.TryEnterWriteLock(TimeSpan.FromSeconds(60)) == false)
                throw new InvalidOperationException(
                    "Cannot modify indexes while indexing is in progress (already waited full minute). Try again later");
            return new DisposableAction(currentlyIndexingLock.ExitWriteLock);
        }


        public bool IsCurrentlyIndexing()
        {
            return Interlocked.Read(ref currentlyIndexing) != 0;
        }

        public IDisposable CurrentlyIndexing()
        {
            currentlyIndexingLock.EnterReadLock();
            Interlocked.Increment(ref currentlyIndexing);
            return new DisposableAction(() =>
            {
                currentlyIndexingLock.ExitReadLock();
                Interlocked.Decrement(ref currentlyIndexing);
            });
        }

        public void RemoveTransformer(string name)
        {
            var transformer = GetTransformerDefinition(name);
            if (transformer == null) return;
            RemoveTransformer(transformer.IndexId);
        }

        public void RemoveIndex(string name)
        {
            var index = GetIndexDefinition(name);
            if (index == null) return;
            RemoveIndex(index.IndexId);
        }

        public void RemoveTransformer(int id)
        {
            AbstractTransformer ignoredViewGenerator;
            int ignoredId;
            if (transformCache.TryRemove(id, out ignoredViewGenerator))
                transformNameToId.TryRemove(ignoredViewGenerator.Name, out ignoredId);
            TransformerDefinition ignoredIndexDefinition;
            transformDefinitions.TryRemove(id, out ignoredIndexDefinition);
            if (configuration.RunInMemory)
                return;
            File.Delete(GetIndexSourcePath(id) + ".transform");
            UpdateTransformerMappingFile();
        }

        public AbstractTransformer GetTransformer(int id)
        {
            AbstractTransformer value;
            if (transformCache.TryGetValue(id, out value) == false)
                return null;
            return value;
        }

        public AbstractTransformer GetTransformer(string name)
        {
            int id = 0;
            if (transformNameToId.TryGetValue(name, out id))
                return transformCache[id];
            return null;
        }

        private void UpdateIndexMappingFile()
        {
            if (configuration.RunInMemory)
                return;

            var sb = new StringBuilder();

            foreach (var index in indexNameToId)
            {
                sb.Append(string.Format("{0} - {1}{2}", index.Value, index.Key, Environment.NewLine));
            }

            File.WriteAllText(Path.Combine(path, "indexes.txt"), sb.ToString());
        }

        private void UpdateTransformerMappingFile()
        {
            if (configuration.RunInMemory)
                return;

            var sb = new StringBuilder();

            foreach (var transform in transformNameToId)
            {
                sb.Append(string.Format("{0} - {1}{2}", transform.Value, transform.Key, Environment.NewLine));
            }

            File.WriteAllText(Path.Combine(path, "transformers.txt"), sb.ToString());
        }
    }

}
