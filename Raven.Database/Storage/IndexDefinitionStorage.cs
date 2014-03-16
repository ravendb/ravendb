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
using Lucene.Net.Documents;
using Microsoft.Isam.Esent.Interop;
using Mono.CSharp;
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
using Raven.Database.Indexing;
using CSharpParser = ICSharpCode.NRefactory.CSharp.CSharpParser;
using Expression = ICSharpCode.NRefactory.CSharp.Expression;

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
                    AddTransform(indexDefinition.TransfomerId, indexDefinition);
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
            transformCache.AddOrUpdate(transformerDefinition.TransfomerId, generator, (s, viewGenerator) => generator);

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
            {
                IndexDefinition indexDefinition = IndexDefinitions[id];
                if (indexDefinition.Fields.Count == 0)
                {
                    AbstractViewGenerator abstractViewGenerator = GetViewGenerator(id);
                    indexDefinition.Fields = abstractViewGenerator.Fields;
                }
                return indexDefinition;
            }
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
            var indexes = ParseIndexesAndGetReadyToMerge();
            var mergedIndexesData = MergeIndexes(indexes);
            var mergedResults = CreateMergeIndexDefinition(mergedIndexesData);
            return mergedResults;
        }

        private List<MergeProposal> MergeIndexes(List<IndexData> indexes)
        {
            var mergedIndexesData = new List<MergeProposal>();
            foreach (var indexData in indexes)
            {
                var mergeData = new MergeProposal();

                if (indexData.IsAlreadyMerged)
                    continue;

                indexData.IsAlreadyMerged = true;

                var failComments = new List<string>();
                if (indexData.IsMapReduced)
                {
                    failComments.Add("Cannot merge indexes containing map  reduce ");
                }

                if (indexData.ExistsMorethan1Maps)
                {
                    failComments.Add("Cannot merge indexes with more than 1 map");
                }

                if (indexData.NumberOfFromClauses > 1)
                {
                    failComments.Add("Cannot merge indexes that have more than 1 from clause");
                }
                if (indexData.NumberOfSelectClauses > 1)
                {
                    failComments.Add("Cannot merge indexes that have more than 1 select clause");
                }
                if (indexData.HasWhere)
                {
                    failComments.Add("Cannot merge indexes that have a where clause");
                }
                if (indexData.HasGroup)
                {
                    failComments.Add("Cannot merge indexes that have a group clause");
                }
                if (indexData.HasLet)
                {
                    failComments.Add("Cannot merge indexes that have a let clause");
                }
                if (indexData.HasOrder)
                {
                    failComments.Add("Cannot merge indexes that have an order clause");
                }
                if (failComments.Count != 0)
                {
                    indexData.Comment = string.Join(Environment.NewLine, failComments);
                    indexData.IsSuitedForMerge = false;
                    var data = new MergeProposal
                    {
                        MergedData = indexData
                    };
                    mergedIndexesData.Add(data);
                    continue;
                }

              
                mergeData.ProposedForMerge.Add(indexData);
                foreach (var curIndexData in indexes)
                {
                    //if (!AreSuitedForMergeCriterions(indexData, curIndexData))
                    //    continue;
                    if (!AreSuitedForMergeCriterions(mergeData.ProposedForMerge, curIndexData))
                        continue;
               
                    if (CompareSelectExpression(curIndexData, indexData))
                    {
                        curIndexData.IsSuitedForMerge = true;
                        mergeData.ProposedForMerge.Add(curIndexData);
                    }
                }

                var newData = new MergeProposal
                {
                    ProposedForMerge = mergeData.ProposedForMerge,
                    MergedData = mergeData.MergedData
                };
                mergedIndexesData.Add(newData);
            }
            return mergedIndexesData;
        }

        private List<IndexData> ParseIndexesAndGetReadyToMerge()
        {
            var parser = new CSharpParser();
            var indexes = new List<IndexData>();

            foreach (var kvp in IndexDefinitions)
            {
                var index = kvp.Value;
                if (index.IsMapReduce)
                {
                    var indexData = new IndexData
                    {
                        IndexId = index.IndexId,
                        IndexName = index.Name,
                        OriginalMap = index.Map,
                        IsMapReduced = true,
                     };


                    indexes.Add(indexData);
                    continue;
                }

                if (index.Maps.Count > 1)  //no support for multiple maps
                {
                    var indexData = new IndexData
                    {
                        IndexId = index.IndexId,
                        IndexName = index.Name,
                        OriginalMap = index.Map,
                        ExistsMorethan1Maps = true,
                     };
                     indexes.Add(indexData);
                     continue;
                }
                   

                var map = parser.ParseExpression(index.Map); // TODO: Multiple maps

                var visitor = new IndexVisitor();
                map.AcceptVisitor(visitor);
                var curIndexData = new IndexData
                {
                    IndexId = index.IndexId,
                    IndexName = index.Name,
                    OriginalMap = index.Map,
                    HasWhere = visitor.HasWhere,
                    HasLet = visitor.HasLet,
                    HasGroup = visitor.HasGroup,
                    HasOrder = visitor.HasOrder,

                    FromExpression = visitor.FromExpression,
                    FromIdentifier = visitor.FromIdentifier,
                    NumberOfFromClauses = visitor.NumberOfFromClauses,
                    SelectExpressions = visitor.SelectExpressions,
                    NumberOfSelectClauses = visitor.NumberOfSelectClauses,
                };

                curIndexData.FillAdditionalProperies(index);
                string res = curIndexData.BuildExpression();
  
                indexes.Add(curIndexData);
            }
            return indexes;
        }
        private bool AreSuitedForMergeCriterions( List<IndexData> indexDataList,IndexData curIndexData)
        {
            foreach (var indexData in indexDataList)
            {
                if (!AreSuitedForMergeCriterions(indexData, curIndexData))
                    return false;
            }
                return true;
        }
        private bool AreSuitedForMergeCriterions( IndexData indexData,IndexData curIndexData)
        {
            if (curIndexData.IndexId == indexData.IndexId)
               return false;

            if (curIndexData.NumberOfFromClauses > 1)
                return false;
          
            if (curIndexData.NumberOfSelectClauses > 1)
                return false;

            //if (curIndexData.IsAlreadyMerged)
            //    return false;
          
            if (curIndexData.HasWhere)
                return false;

            if (curIndexData.HasGroup)
                return false;
            if (curIndexData.HasOrder)
                return false;
            if (curIndexData.HasLet)
                return false;

            if ((curIndexData.FromExpression == null) && (indexData.FromExpression != null))
                return false;

            if ((curIndexData.FromExpression != null) && (indexData.FromExpression == null))
                return false;

            if ((curIndexData.FromExpression != null) && (indexData.FromExpression != null))
                if (!curIndexData.FromExpression.ToString().Equals(indexData.FromExpression.ToString()))
                    return false;

            if(!CompareAdditionalIndexProperties(indexData, curIndexData))
                    return false;

       
     
            //if(DataDictionaryCompare(indexData.Stores,curIndexData.Stores) == false)
            //    return false;        
            //if (DataDictionaryCompare(indexData.Analyzers, curIndexData.Analyzers) == false)
            //    return false;
            //if (DataDictionaryCompare(indexData.Suggestions, curIndexData.Suggestions) == false)
            //    return false;
            //if (DataDictionaryCompare(indexData.SortOptions, curIndexData.SortOptions) == false)
            //    return false;
            //if (DataDictionaryCompare(indexData.Indexes, curIndexData.Indexes) == false)
            //    return false;
            //if (DataDictionaryCompare(indexData.TermVectors, curIndexData.TermVectors) == false)
            //    return false;
            //if (DataDictionaryCompare(indexData.SpatialIndexes, curIndexData.SpatialIndexes) == false)
            //    return false;

   
            return true;
 
        }

        private bool CompareAdditionalIndexProperties(IndexData index1Data,IndexData index2Data)
        {
            //IEnumerable<string> differentNames12 = index1Data.Fields.Except(index2Data.Fields);
            //IEnumerable<string> differentNames21 = index2Data.Fields.Except(index1Data.Fields);
            IEnumerable<string> intersectNames = index2Data.SelectExpressions.Keys.Intersect(index1Data.SelectExpressions.Keys);


            //foreach (var name in differentNames12)
            //{
            //    if (!index2Data.SelectExpressions.ContainsKey(name))
            //        continue;
            //    //if contains and not in fields - defined with default values. ckeck if second one is also default
            //    if (!AreIndexPropertiesDefault(index1Data, name))
            //        return false;
            //}

            //foreach (var name in differentNames21)
            //{
            //    if (!index1Data.SelectExpressions.ContainsKey(name))
            //        continue;
            //    //if contains and not in fields - defined with default values.ckeck if second one is also default
            //    if (!AreIndexPropertiesDefault(index2Data, name))
            //        return false;
            //}



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

        private bool AreIndexPropertiesDefault(IndexData indexData, string name)
        {
            
            FieldStorage valueStorage;
            if (indexData.Stores.TryGetValue(name, out valueStorage))
            {
                if(valueStorage != FieldStorage.No)
                    return false;
            }

            SortOptions valueSortOptions;
            if (indexData.SortOptions.TryGetValue(name, out valueSortOptions))
            {
                if (valueSortOptions != SortOptions.None)
                    return false;
            }
            FieldTermVector valueTermVectors;
            if (indexData.TermVectors.TryGetValue(name, out valueTermVectors))
            {
                if (valueTermVectors != FieldTermVector.No)
                    return false;
            }
            FieldIndexing valueIndexing;
            if (indexData.Indexes.TryGetValue(name, out valueIndexing))
            {
                if (valueIndexing != FieldIndexing.Default)
                    return false;
            }
            string valueAnalyzer;
            if (indexData.Analyzers.TryGetValue(name, out valueAnalyzer))
            {
                if (!valueAnalyzer.Equals(string.Empty))
                    return false;
            }
            SuggestionOptions valueSuggestionOptions;
            var defaultSuggestionOptions=new SuggestionOptions();
            defaultSuggestionOptions.Distance = StringDistanceTypes.None;
            if (indexData.Suggestions.TryGetValue(name, out valueSuggestionOptions))
            {
                if (!valueSuggestionOptions.Equals(defaultSuggestionOptions))
                    return false;
            }
          


            return true;
        }

        bool IsDefaultValue(FieldStorage val)
        {
            return val == FieldStorage.No;
        }
        bool IsDefaultValue(SortOptions val)
        {
            return val == SortOptions.None;
        }
        bool IsDefaultValue(FieldTermVector val)
        {
            return val == FieldTermVector.No;
        }
        bool IsDefaultValue(FieldIndexing val)
        {
            return val == FieldIndexing.Default;
        }
        bool IsDefaultValue(string val)
        {
            return val.Equals(string.Empty);
        }
        bool IsDefaultValue(SuggestionOptions val)
        {
            var defaultSuggestionOptions = new SuggestionOptions();
            defaultSuggestionOptions.Distance = StringDistanceTypes.None;
 
            return val.Equals(defaultSuggestionOptions);
        }
         bool IsDefaultValue<T>(T val)
        {
             var type = typeof(T);
             var valAsString = val as string;
             if (valAsString !=null)
                 return IsDefaultValue(valAsString);
                         
            var valAsSuggestion = val as SuggestionOptions;
            if ( valAsSuggestion!=null)
                 return IsDefaultValue(valAsSuggestion);

             if (type.IsEnum)
             {
                 if (type.FullName.Equals(typeof (SortOptions).FullName))
                 {
                     var valAsSortOption = (SortOptions) Convert.ChangeType(val, typeof (SortOptions));
                     return IsDefaultValue(valAsSortOption);
                 }
                 if (type.FullName.Equals(typeof (FieldStorage).FullName))
                 {
                     var valAsStorage = (FieldStorage) Convert.ChangeType(val, typeof (FieldStorage));
                     return IsDefaultValue(valAsStorage);
                 }
                 if (type.FullName.Equals(typeof (FieldTermVector).FullName))
                 {
                     var valAsTermVector = (FieldTermVector) Convert.ChangeType(val, typeof (FieldTermVector));
                     return IsDefaultValue(valAsTermVector);
                 }

                 if (type.FullName.Equals(typeof (FieldIndexing).FullName))
                 {
                     var valAsIndexing = (FieldIndexing) Convert.ChangeType(val, typeof (FieldIndexing));
                     return IsDefaultValue(valAsIndexing);
                 }
             }
             return true;
        }
        //private bool DataDictionaryCompare<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2)
        //{
   
        //    foreach (var kvp in dataDict1)
        //    {
        //        T v2;
        //        if (!dataDict2.TryGetValue(kvp.Key, out v2))
        //            continue;
        //        if (Equals(kvp.Value, v2) == false)
        //            return false;
        //    }
        //    return true;
        //}
        private bool DataDictionaryCompare<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2,IEnumerable<string> names )
        {
             
            //IEnumerable<string> differentNames12 = dataDict1.Keys.Except(dataDict2.Keys);
            //IEnumerable<string> differentNames21 = dataDict2.Keys.Except( dataDict1.Keys);
            //IEnumerable<string> intersectNames =  dataDict1.Keys.Intersect(dataDict2.Keys);

            bool found1, found2;

            //foreach (var name in differentNames12)
            //{
            //    if (!index2Data.SelectExpressions.ContainsKey(name))
            //        continue;
            //    //if contains and not in fields - defined with default values. ckeck if second one is also default
            //    if (!AreIndexPropertiesDefault(index1Data, name))
            //        return false;
            //}

            //foreach (var name in differentNames21)
            //{
            //    if (!index1Data.SelectExpressions.ContainsKey(name))
            //        continue;
            //    //if contains and not in fields - defined with default values.ckeck if second one is also default
            //    if (!AreIndexPropertiesDefault(index2Data, name))
            //        return false;
            //}

            foreach (var kvp in names)
            {
                T v1,v2;
                found1 = dataDict1.TryGetValue(kvp, out v1);
                found2 = dataDict2.TryGetValue(kvp, out v2);

                    
                if (found1 && found2 && Equals(v1, v2) == false)
                    return false;

               // exists only in 1 - check if contains default value
                if (found1 && !found2)
                
                {
                    if (! IsDefaultValue(v1))
                        return false;
                }
                if (found2 && !found1)
                    {
                        if (!IsDefaultValue(v2))
                            return false;
                    }
   
           }
            //foreach (var kvp in differentNames21)
            //{
            //    T v1, v2;
            //  //  found1 = dataDict1.TryGetValue(kvp, out v1);
            //    found2 = dataDict2.TryGetValue(kvp, out v2);


            //  //  if (found1 && found2 && Equals(v1, v2) == false)
            //  //      return false;

            //    //exists only in 1 - check if contains default value
            //    //if (found1 && !found2)
            //    //{
            //    //    if (!IsDefaultValue(v1))
            //    //        return false;
            //    //}
            //  //  if (found2 && !found1)
            //    if (found2 )
            //    {
            //        if (!IsDefaultValue(v2))
            //            return false;
            //    }

            //}

            //foreach (var kvp in intersectNames)
            //{
            //    T v1, v2;
            //    found1 = dataDict1.TryGetValue(kvp, out v1);
            //    found2 = dataDict2.TryGetValue(kvp, out v2);


            //    if (found1 && found2 && Equals(v1, v2) == false)
            //        return false;

            //    //exists only in 1 - check if contains default value
            //    if (found1 && !found2)
            //    {
            //        if (!IsDefaultValue(v1))
            //            return false;
            //    }
            //    if (found2 && !found1)
            //    {
            //        if (!IsDefaultValue(v2))
            //            return false;
            //    }

            //}
 
          
            return true;
        }
        private IDictionary<string, T>  DataDictionaryMerge<T>(IDictionary<string, T> dataDict1,IDictionary<string, T> dataDict2)
            {
            var resultDictionary = new Dictionary<string, T>();
    
            foreach (var curExpr in dataDict1.Keys.Where(curExpr => !resultDictionary.ContainsKey(curExpr)))
            {
                resultDictionary.Add(curExpr, dataDict1[curExpr]); 
            }
            foreach (var curExpr in dataDict2.Keys.Where(curExpr => !resultDictionary.ContainsKey(curExpr)))
            {
                resultDictionary.Add(curExpr, dataDict2[curExpr]);  
            }
           
            return resultDictionary;
        }
        

        private bool CompareSelectExpression(IndexData expr1, IndexData expr2)
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
                        return false;

                    }
                }
            }
            return true;
        }

        private IndexMergeResults CreateMergeIndexDefinition(List<MergeProposal> indexDataForMerge)
        {
            var indexMergeResults = new IndexMergeResults();
            foreach (var mergeProposal in indexDataForMerge)
            {
                if (mergeProposal.ProposedForMerge.Count > 1)
                    continue;
                if (mergeProposal.MergedData == null) 
                    continue;
                indexMergeResults.Unmergables.Add(mergeProposal.MergedData.IndexName,mergeProposal.MergedData.Comment);
            }
            foreach (var mergeProposal in indexDataForMerge)
            {
                if (mergeProposal.ProposedForMerge.Count == 0)
                    continue;
                
                var indexData = mergeProposal.ProposedForMerge.First();

                var mergeSuggestion = new MergeSuggestions
                {
                    CanMerge = { indexData.IndexName},                          

                };
                foreach (var field in indexData.Stores)
                {
                    mergeSuggestion.MergedIndex.Stores.Add(field);
                }
                foreach (var field in indexData.Indexes)
                {
                    mergeSuggestion.MergedIndex.Indexes.Add(field);
                }
                foreach (var field in indexData.Analyzers)
                {
                    mergeSuggestion.MergedIndex.Analyzers.Add(field);
                }
                foreach (var field in indexData.SortOptions)
                {
                    mergeSuggestion.MergedIndex.SortOptions.Add(field);
                }
                foreach (var field in indexData.Suggestions)
                {
                    mergeSuggestion.MergedIndex.Suggestions.Add(field);
                }
                foreach (var field in indexData.TermVectors)
                {
                    mergeSuggestion.MergedIndex.TermVectors.Add(field);
                }
                foreach (var field in indexData.SpatialIndexes)
                {
                    mergeSuggestion.MergedIndex.SpatialIndexes.Add(field);
                }
              


             
               var selectExpressionDict = new Dictionary<string, Expression>();
                   
                foreach (var curProposedData in mergeProposal.ProposedForMerge)
                {
                      foreach (var curExpr in curProposedData.SelectExpressions.Where(curExpr => !selectExpressionDict.ContainsKey(curExpr.Key)))
                        {
                            selectExpressionDict.Add(curExpr.Key,curExpr.Value);  
                        }
                        if (!string.Equals(indexData.IndexName, curProposedData.IndexName))
                        {
                            mergeSuggestion.CanMerge.Add(curProposedData.IndexName);
                            mergeSuggestion.MergedIndex.Stores = DataDictionaryMerge(mergeSuggestion.MergedIndex.Stores, curProposedData.Stores);
                            mergeSuggestion.MergedIndex.Indexes = DataDictionaryMerge(mergeSuggestion.MergedIndex.Indexes, curProposedData.Indexes);
                            mergeSuggestion.MergedIndex.Analyzers = DataDictionaryMerge(mergeSuggestion.MergedIndex.Analyzers, curProposedData.Analyzers);
                            mergeSuggestion.MergedIndex.SortOptions = DataDictionaryMerge(mergeSuggestion.MergedIndex.SortOptions, curProposedData.SortOptions);
                            mergeSuggestion.MergedIndex.Suggestions = DataDictionaryMerge(mergeSuggestion.MergedIndex.Suggestions, curProposedData.Suggestions);
                            mergeSuggestion.MergedIndex.TermVectors = DataDictionaryMerge(mergeSuggestion.MergedIndex.TermVectors, curProposedData.TermVectors);
                            mergeSuggestion.MergedIndex.SpatialIndexes = DataDictionaryMerge(mergeSuggestion.MergedIndex.SpatialIndexes, curProposedData.SpatialIndexes);


                            //var exceptFields = curProposedData.Fields.Except(indexData.Fields);
                            //foreach (string field in exceptFields)
                            //{
                            //    mergeSuggestion.MergedIndex.Fields.Add(field);
                            //}
   
                         
                        }
                }
                indexData.SelectExpressions = selectExpressionDict;
                string resSuggestion = indexData.BuildExpression();
 
                mergeSuggestion.MergedIndex.Name = indexData.IndexName;
                mergeSuggestion.MergedIndex.IndexId = indexData.IndexId;
                mergeSuggestion.MergedIndex.Map = resSuggestion;

                //IDictionary<string, T>  DataDictionaryMerge<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2)
                //mergeSuggestion.MergedIndex.Fields = indexData.Fields;
                   // mergeSuggestion.MergedIndex.Stores = indexData.Stores;
            
                   // mergeSuggestion.MergedIndex.Indexes = indexData.Indexes;
              
                   //mergeSuggestion.MergedIndex.Analyzers= indexData.Analyzers;
      
                   // mergeSuggestion.MergedIndex.SortOptions = indexData.SortOptions;
            
                   // mergeSuggestion.MergedIndex.Suggestions = indexData.Suggestions;
             
                   // mergeSuggestion.MergedIndex.TermVectors = indexData.TermVectors;
            
                   // mergeSuggestion.MergedIndex.SpatialIndexes = indexData.SpatialIndexes;
                if (mergeProposal.ProposedForMerge.Count > 1)
                {
                    indexMergeResults.Suggestions.Add(mergeSuggestion);
                }
                if ((mergeProposal.ProposedForMerge.Count == 1) &&(indexData.IsSuitedForMerge==false))
                {
                   
                    const string comment = "Can't find any entity name for merge";
                    indexMergeResults.Unmergables.Add(mergeSuggestion.MergedIndex.Name, comment);
                    //indexMergeResults.Other.Add(mergeSuggestion);
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
            //bool hasMatch = false;
            //foreach (var sug1 in originalIndexes.Suggestions)
            //{
            //    foreach (var sug2 in originalIndexes.Suggestions)
            //    {
            //        if ((sug1 != sug2) && (sug1.CanMerge.Count <= sug2.CanMerge.Count ))
            //        {
            //            var sugCanMergeSet = new HashSet<string>(sug1.CanMerge);

            //            if ((hasMatch = sugCanMergeSet.IsSubsetOf(sug2.CanMerge)))
            //            {
            //                break;
            //            }

            //        }
            //    }
            //    if (!hasMatch)
            //    {
            //        resultingIndexMerge.Suggestions.Add(sug1);
            //        hasMatch = false;
            //    }
            //}
            bool hasMatch = false;
            for (var i = 0; i < originalIndexes.Suggestions.Count; i++)
            {
                var sug1 = originalIndexes.Suggestions[i];
                for (var j = i + 1; j < originalIndexes.Suggestions.Count; j++)
                {
                    var sug2 = originalIndexes.Suggestions[j];
                    if ((sug1 != sug2) && (sug1.CanMerge.Count <= sug2.CanMerge.Count))
                    {
                        var sugCanMergeSet = new HashSet<string>(sug1.CanMerge);
                        if ((hasMatch = sugCanMergeSet.IsSubsetOf(sug2.CanMerge)))
                        {
                            break;
                        }

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
            RemoveTransformer(transformer.TransfomerId);
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
