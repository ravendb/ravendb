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

namespace Raven.Database.Storage
{
    public class IndexDefinitionStorage
    {
        private const string IndexDefDir = "IndexDefinitions";

        private readonly ReaderWriterLockSlim currentlyIndexingLock = new ReaderWriterLockSlim();

        private readonly ConcurrentDictionary<string, AbstractViewGenerator> indexCache =
            new ConcurrentDictionary<string, AbstractViewGenerator>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, AbstractTransformer> transformCache =
            new ConcurrentDictionary<string, AbstractTransformer>(StringComparer.InvariantCultureIgnoreCase);

        private readonly ConcurrentDictionary<string, TransformerDefinition> transformDefinitions =
            new ConcurrentDictionary<string, TransformerDefinition>(StringComparer.InvariantCultureIgnoreCase);


        private readonly ConcurrentDictionary<string, IndexDefinition> indexDefinitions =
            new ConcurrentDictionary<string, IndexDefinition>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, IndexDefinition> newDefinitionsThisSession = new ConcurrentDictionary<string, IndexDefinition>(StringComparer.InvariantCultureIgnoreCase);

        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        private readonly string path;
        private readonly InMemoryRavenConfiguration configuration;
        private readonly OrderedPartCollection<AbstractDynamicCompilationExtension> extensions;

        public IndexDefinitionStorage(
            InMemoryRavenConfiguration configuration,
            ITransactionalStorage transactionalStorage,
            string path,
            IEnumerable<AbstractViewGenerator> compiledGenerators,
            OrderedPartCollection<AbstractDynamicCompilationExtension> extensions)
        {
            this.configuration = configuration;
            this.extensions = extensions; // this is used later in the ctor, so it must appears first
            this.path = Path.Combine(path, IndexDefDir);

            if (Directory.Exists(this.path) == false && configuration.RunInMemory == false)
                Directory.CreateDirectory(this.path);

            if (configuration.RunInMemory == false)
                ReadFromDisk();

            //compiled view generators always overwrite dynamic views
            ReadIndexesFromCatalog(compiledGenerators, transactionalStorage);

            newDefinitionsThisSession.Clear();
        }

        public bool IsNewThisSession(IndexDefinition definition)
        {
	        var fixedName = FixupIndexName(definition.Name);
			return newDefinitionsThisSession.ContainsKey(fixedName);
        }

        private void ReadIndexesFromCatalog(IEnumerable<AbstractViewGenerator> compiledGenerators, ITransactionalStorage transactionalStorage)
        {
            foreach (var generator in compiledGenerators)
            {
                var copy = generator;
                var displayNameAtt = TypeDescriptor.GetAttributes(copy)
                    .OfType<DisplayNameAttribute>()
                    .FirstOrDefault();

                var name = displayNameAtt != null ? displayNameAtt.DisplayName : copy.GetType().Name;

	            name = FixupIndexName(name, path);

                transactionalStorage.Batch(actions =>
                {
                    if (actions.Indexing.GetIndexesStats().Any(x => x.Name == name))
                        return;

                    actions.Indexing.AddIndex(name, copy.ReduceDefinition != null);
                });

                var indexDefinition = new IndexDefinition
                {
                    Name = name,
                    Map = "Compiled map function: " + generator.GetType().AssemblyQualifiedName,
                    // need to supply this so the index storage will create map/reduce index
                    Reduce = generator.ReduceDefinition == null ? null : "Compiled reduce function: " + generator.GetType().AssemblyQualifiedName,
                    Indexes = generator.Indexes,
                    Stores = generator.Stores,
                    TermVectors = generator.TermVectors,
                    SpatialIndexes = generator.SpatialIndexes,
                    IsCompiled = true
                };
                indexCache.AddOrUpdate(name, copy, (s, viewGenerator) => copy);
                indexDefinitions.AddOrUpdate(name, indexDefinition, (s1, definition) => indexDefinition);
            }
        }

        private void ReadFromDisk()
        {
            foreach (var index in Directory.GetFiles(path, "*.index"))
            {
                try
                {
                    var indexDefinition = JsonConvert.DeserializeObject<IndexDefinition>(File.ReadAllText(index), Default.Converters);
                    if (indexDefinition.Name == null)
                        indexDefinition.Name = MonoHttpUtility.UrlDecode(Path.GetFileNameWithoutExtension(index));
                    ResolveAnalyzers(indexDefinition);
                    AddAndCompileIndex(indexDefinition);
                    AddIndex(indexDefinition.Name, indexDefinition);
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
					var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(File.ReadAllText(transformer), Default.Converters);
                    if (transformerDefinition.Name == null)
						transformerDefinition.Name = MonoHttpUtility.UrlDecode(Path.GetFileNameWithoutExtension(transformer));
                    AddAndCompileTransform(transformerDefinition);
                    AddTransform(transformerDefinition.Name, transformerDefinition);
                }
                catch (Exception e)
                {
					logger.WarnException("Could not compile transformer " + transformer + ", skipping bad transformer", e);
                }
            }
        }

        public int IndexesCount
        {
            get { return indexCache.Count; }
        }

        public string[] IndexNames
        {
            get { return indexCache.Keys.OrderBy(name => name).ToArray(); }
        }

        public string IndexDefinitionsPath
        {
            get { return path; }
        }

        public string[] TransformerNames
        {
            get { return transformDefinitions.Keys.OrderBy(name => name).ToArray(); }
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
			var fixedName = FixupIndexName(indexDefinition.Name);
			var indexName = Path.Combine(path, MonoHttpUtility.UrlEncode(fixedName) + ".index");
            // Hash the name if it's too long (as a path)
            File.WriteAllText(indexName, JsonConvert.SerializeObject(indexDefinition, Formatting.Indented, Default.Converters));
        }

		private void WriteTransformerDefinition(TransformerDefinition transformerDefinition)
		{
			if (configuration.RunInMemory)
				return;
			var fixedName = FixupIndexName(transformerDefinition.Name);
			var indexName = Path.Combine(path, MonoHttpUtility.UrlEncode(fixedName) + ".transform");
			// Hash the name if it's too long (as a path)
			File.WriteAllText(indexName, JsonConvert.SerializeObject(transformerDefinition, Formatting.Indented, Default.Converters));
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
			var fixedName = FixupIndexName(definition.Name);

			indexDefinitions.AddOrUpdate(fixedName, s => 
            {
                throw new InvalidOperationException("Cannot find index named: " + definition.Name);
            }, (s, indexDefinition) => definition);
            WriteIndexDefinition(definition);
        }

        private DynamicViewCompiler AddAndCompileIndex(IndexDefinition indexDefinition)
        {
			var fixedName = FixupIndexName(indexDefinition.Name);
			var transformer = new DynamicViewCompiler(fixedName, indexDefinition, extensions, path, configuration);
            var generator = transformer.GenerateInstance();
			indexCache.AddOrUpdate(fixedName, generator, (s, viewGenerator) => generator);

            logger.Info("New index {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name, transformer.CompiledQueryText,
                              transformer.CompiledQueryText);
            return transformer;
        }

        private DynamicTransformerCompiler AddAndCompileTransform(TransformerDefinition transformerDefinition)
        {
			var fixedName = FixupIndexName(transformerDefinition.Name);
			var transformer = new DynamicTransformerCompiler(transformerDefinition, configuration, extensions, fixedName, path);
            var generator = transformer.GenerateInstance();
			transformCache.AddOrUpdate(fixedName, generator, (s, viewGenerator) => generator);

            logger.Info("New transformer {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name, transformer.CompiledQueryText,
                              transformer.CompiledQueryText);
            return transformer;
        }

		public void RegisterNewIndexInThisSession(string name, IndexDefinition definition)
		{
			var fixedName = FixupIndexName(name);
			newDefinitionsThisSession.TryAdd(fixedName, definition);
		}

        public void AddIndex(string name, IndexDefinition definition)
        {
			var fixedName = FixupIndexName(name);
			indexDefinitions.AddOrUpdate(fixedName, definition, (s1, def) =>
            {
                if (def.IsCompiled)
                    throw new InvalidOperationException("Index " + name + " is a compiled index, and cannot be replaced");
                return definition;
            });
        }

        public void AddTransform(string name, TransformerDefinition definition)
        {
			var fixedName = FixupIndexName(name);
			transformDefinitions.AddOrUpdate(fixedName, definition, (s1, def) => definition);
        }

        public void RemoveIndex(string name)
        {
			var fixedName = FixupIndexName(name);
            AbstractViewGenerator ignoredViewGenerator;
			indexCache.TryRemove(fixedName, out ignoredViewGenerator);
            IndexDefinition ignoredIndexDefinition;
			indexDefinitions.TryRemove(fixedName, out ignoredIndexDefinition);
			newDefinitionsThisSession.TryRemove(fixedName, out ignoredIndexDefinition);
            if (configuration.RunInMemory)
                return;
			File.Delete(GetIndexSourcePath(fixedName) + ".index");
        }

        private string GetIndexSourcePath(string name)
        {
			var fixedName = FixupIndexName(name, path);
			return Path.Combine(path, MonoHttpUtility.UrlEncode(fixedName));
        }

        public IndexDefinition GetIndexDefinition(string name)
        {
			var fixedName = FixupIndexName(name);
            IndexDefinition value;
			indexDefinitions.TryGetValue(fixedName, out value);
            if (value != null && value.Name == null) // backward compact, mostly
                value.Name = name;
            return value;
        }

        public TransformerDefinition GetTransformerDefinition(string name)
        {
			var fixedName = FixupIndexName(name);
            TransformerDefinition value;
			transformDefinitions.TryGetValue(fixedName, out value);
            if (value != null && value.Name == null) // backward compact, mostly
                value.Name = name;
            return value;
        }

        public AbstractViewGenerator GetViewGenerator(string name)
        {
			var fixedName = FixupIndexName(name);
            AbstractViewGenerator value;
			if (indexCache.TryGetValue(fixedName, out value) == false)
                return null;
            return value;
        }

        public IndexCreationOptions FindIndexCreationOptions(IndexDefinition indexDef)
        {
            var indexDefinition = GetIndexDefinition(indexDef.Name);
            if (indexDefinition != null)
            {
                return indexDefinition.Equals(indexDef)
                    ? IndexCreationOptions.Noop
                    : IndexCreationOptions.Update;
            }
            return IndexCreationOptions.Create;
        }

        public bool Contains(string indexName)
        {
			var fixedName = FixupIndexName(indexName);
			return indexDefinitions.ContainsKey(fixedName);
        }

        private string FixupIndexName(string index)
        {
            return FixupIndexName(index, path);
        }

        public static string FixupIndexName(string index, string path)
        {
	        if (index.EndsWith("=")) //allready encoded
		        return index;
            index = index.Trim();
            string prefix = null;
            if (index.StartsWith("Temp/") || index.StartsWith("Auto/"))
            {
                prefix = index.Substring(0, 5);
            }
            if (path.Length + index.Length > 230 ||
                Encoding.Unicode.GetByteCount(index) >= 255)
            {
                using (var md5 = MD5.Create())
                {
                    var bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(index));
                    var result = prefix + Convert.ToBase64String(bytes);

					if(path.Length + result.Length > 230)
						throw new InvalidDataException("index name with the given path is too long even after encoding: " + index);

	                return result;
                }
            }
            return index;
        }

        public static void ResolveAnalyzers(IndexDefinition indexDefinition)
        {
            // Stick Lucene.Net's namespace to all analyzer aliases that are missing a namespace
            var analyzerNames = (from analyzer in indexDefinition.Analyzers
                                 where analyzer.Value.IndexOf('.') == -1
                                 select analyzer).ToArray();

            // Only do this for analyzer that actually exist; we do this here to be able to throw a correct error later on
            foreach (var a in analyzerNames.Where(a => typeof(StandardAnalyzer).Assembly.GetType("Lucene.Net.Analysis." + a.Value) != null))
            {
                indexDefinition.Analyzers[a.Key] = "Lucene.Net.Analysis." + a.Value;
            }
        }

        public IDisposable TryRemoveIndexContext()
        {
            if (currentlyIndexingLock.TryEnterWriteLock(TimeSpan.FromSeconds(60)) == false)
                throw new InvalidOperationException("Cannot modify indexes while indexing is in progress (already waited full minute). Try again later");
            return new DisposableAction(currentlyIndexingLock.ExitWriteLock);
        }

        public IDisposable CurrentlyIndexing()
        {
            currentlyIndexingLock.EnterReadLock();

            return new DisposableAction(currentlyIndexingLock.ExitReadLock);
        }

        public void RemoveTransformer(string name)
        {
			var fixedName = FixupIndexName(name);
            AbstractTransformer ignoredViewGenerator;
			transformCache.TryRemove(fixedName, out ignoredViewGenerator);
            TransformerDefinition ignoredIndexDefinition;
			transformDefinitions.TryRemove(fixedName, out ignoredIndexDefinition);
            if (configuration.RunInMemory)
                return;
			File.Delete(GetIndexSourcePath(fixedName) + ".transform");
        }

        public AbstractTransformer GetTransformer(string name)
        {
			var fixedName = FixupIndexName(name);
            AbstractTransformer value;
			if (transformCache.TryGetValue(fixedName, out value) == false)
                return null;
            return value;
        }
    }
}
