using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using log4net;
using Rhino.DivanDB.Json;
using Rhino.DivanDB.Linq;
using System.Linq;

namespace Rhino.DivanDB.Storage
{
    public class IndexDefinitionStorage
    {
        private const string IndexDefDir = "IndexDefinitions";
        private readonly string path;
        private readonly IDictionary<string, IndexingFunc> indexCache = new Dictionary<string, IndexingFunc>();
        private readonly ILog logger = LogManager.GetLogger(typeof (IndexDefinitionStorage));

        public IndexDefinitionStorage(string path)
        {
            this.path = Path.Combine(path, IndexDefDir);

            if (Directory.Exists(this.path) == false)
                Directory.CreateDirectory(this.path);

            foreach (var index in Directory.GetFiles(this.path, "*.index"))
            {
                var viewDefinition = File.ReadAllText(index);
                try
                {
                    CompileIndex(viewDefinition);
                }
                catch (Exception e)
                {
                    logger.Warn("Could not compile index " + index +", skipping bad index", e);
                }
            }
        }

        public string[] IndexNames
        {
            get { return indexCache.Keys.ToArray(); }
        }

        public string AddIndex(LinqTransformer transformer)
        {
            var generator = (AbstractViewGenerator)Activator.CreateInstance(transformer.CompiledType);
            indexCache[transformer.Name] = generator.CompiledDefinition;
            File.WriteAllText(Path.Combine(path,transformer.Name + ".index"), transformer.Source);
            return transformer.Name;
        }

        private LinqTransformer CompileIndex(string indexDef)
        {
            var transformer = new LinqTransformer(indexDef, "docs", path, typeof(JsonDynamicObject));
            transformer.Compile();
            return transformer;
        }

        public void RemoveIndex(string name)
        {
            indexCache.Remove(name);
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
            var viewFile = GetIndexPath(name);
            if(File.Exists(viewFile) == false)
                throw new InvalidOperationException("Index file does not exists");
            return File.ReadAllText(viewFile);
        }

        public IndexingFunc GetIndexingFunction(string name)
        {
            IndexingFunc value;
            if(indexCache.TryGetValue(name, out value)==false)
                return null;
            return value;
        }

        public IndexCreationOptions FindIndexCreationOptionsOptions(string indexDef, out LinqTransformer transformer)
        {
            transformer = CompileIndex(indexDef);
            if(indexCache.ContainsKey(transformer.Name))
            {
                return GetIndexDefinition(transformer.Name) == indexDef
                           ? IndexCreationOptions.Noop
                           : IndexCreationOptions.Update;
            }
            return IndexCreationOptions.Create;
        }
    }
}