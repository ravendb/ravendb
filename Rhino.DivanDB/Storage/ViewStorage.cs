using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using log4net;
using Rhino.DivanDB.Json;
using Rhino.DivanDB.Linq;

namespace Rhino.DivanDB.Storage
{
    public class ViewStorage
    {
        private readonly string path;
        private readonly IDictionary<string, ViewFunc> viewsCache = new Dictionary<string, ViewFunc>();
        private readonly ILog logger = LogManager.GetLogger(typeof (ViewStorage));

        public ViewStorage(string path)
        {
            this.path = Path.Combine(path, "Views");

            if (Directory.Exists(this.path) == false)
                Directory.CreateDirectory(this.path);

            foreach (var view in Directory.GetFiles(this.path, "*.view"))
            {
                var viewDefinition = File.ReadAllText(view);
                try
                {
                    CompileViewDefinition(viewDefinition);
                }
                catch (Exception e)
                {
                    logger.Warn("Could not compile view " + view +", skipping bad view", e);
                }
            }
        }

        public ViewFunc AddView(string viewDefinition)
        {
            var transformer = CompileViewDefinition(viewDefinition);
            File.WriteAllText(Path.Combine(path,transformer.Name + ".view"), viewDefinition);
            return viewsCache[transformer.Name];
        }

        private LinqTransformer CompileViewDefinition(string viewDefinition)
        {
            var transformer = new LinqTransformer(viewDefinition, "docs", path, typeof(JsonDynamicObject));
            Type type = transformer.Compile();
            var generator = (AbstractViewGenerator)Activator.CreateInstance(type);
            viewsCache[transformer.Name] = generator.CompiledDefinition;
            return transformer;
        }

        public IEnumerable AllViews(IEnumerable<JsonDynamicObject> source)
        {
            foreach (var viewFunc in viewsCache.Values)
            {
                foreach (var doc in viewFunc(source))
                {
                    yield return doc;
                }
            }
        }

        public void RemoveView(string name)
        {
            viewsCache.Remove(name);
            File.Delete(Path.Combine(path, name+".view"));
            File.Delete(Path.Combine(path, name + ".view.cs"));
        }
    }
}