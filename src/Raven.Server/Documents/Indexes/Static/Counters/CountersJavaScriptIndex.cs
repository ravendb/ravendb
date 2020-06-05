using System.Collections.Generic;
using Jint;
using Jint.Native.Object;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CountersJavaScriptIndex : AbstractJavaScriptIndex
    {
        public CountersJavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration)
            : base(definition, configuration)
        {
        }

        protected override string MapCode => throw new System.NotImplementedException();

        protected override void OnInitializeEngine(Engine engine)
        {
        }

        protected override void ProcessMaps(ObjectInstance definitions, JintPreventResolvingTasksReferenceResolver resolver, List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions)
        {
            collectionFunctions = null;
        }
    }
}
