using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Raven.Database.Linq;

namespace Raven.Database.Storage
{
    public class IndexDefinitionStorage
    {
        private const string IndexDefDir = "IndexDefinitions";
        private readonly ConcurrentDictionary<string,AbstractViewGenerator> indexCache = new ConcurrentDictionary<string, AbstractViewGenerator>();
        private readonly ILog logger = LogManager.GetLogger(typeof (IndexDefinitionStorage));
        private readonly string path;

        public IndexDefinitionStorage(string path)
        {
            this.path = Path.Combine(path, IndexDefDir);

            if (Directory.Exists(this.path) == false)
                Directory.CreateDirectory(this.path);

            foreach (var index in Directory.GetFiles(this.path, "*.index"))
            {
                var indexDef = File.ReadAllText(index);
                try
                {
                    AddIndex(Path.GetFileNameWithoutExtension(index), indexDef);
                }
                catch (Exception e)
                {
                    logger.Warn("Could not compile index " + index + ", skipping bad index", e);
                }
            }
        }

        public string[] IndexNames
        {
            get { return indexCache.Keys.ToArray(); }
        }

        public string AddIndex(string name, string indexDef)
        {
            var transformer = new DynamicIndexCompiler(name, indexDef);
            var generator = transformer.CreateInstance();
            indexCache.AddOrUpdate(name, generator, (s, viewGenerator) => generator);
            File.WriteAllText(Path.Combine(path, transformer.Name + ".index"), transformer.Query);
            logger.InfoFormat("New index {0}:\r\n{1}\r\nCompiled to:\r\n{2}", transformer.Name, transformer.Query,
                              transformer.CompiledQueryText);
            return transformer.Name;
        }

        public void RemoveIndex(string name)
        {
            AbstractViewGenerator _;
            indexCache.TryRemove(name, out _);
            File.Delete(GetIndexPath(name));
            File.Delete(GetIndexSourcePath(name));
        }

        private string GetIndexSourcePath(string name)
        {
            return Path.Combine(path, name + ".index.cs");
        }

        private string GetIndexPath(string name)
        {
            return Path.Combine(path, name + ".index");
        }

        public string GetIndexDefinition(string name)
        {
            var indexPath = GetIndexPath(name);
            if (File.Exists(indexPath) == false)
                throw new InvalidOperationException("Index file does not exists");
            return File.ReadAllText(indexPath);
        }

        public IndexingFunc GetIndexingFunction(string name)
        {
            AbstractViewGenerator value;
            if (indexCache.TryGetValue(name, out value) == false)
                return null;
            return value.MapDefinition;
        }

        public IndexCreationOptions FindIndexCreationOptionsOptions(string name, string indexDef)
        {
            if (indexCache.ContainsKey(name))
            {
                return GetIndexDefinition(name) == indexDef
                           ? IndexCreationOptions.Noop
                           : IndexCreationOptions.Update;
            }
            return IndexCreationOptions.Create;
        }
    }
}