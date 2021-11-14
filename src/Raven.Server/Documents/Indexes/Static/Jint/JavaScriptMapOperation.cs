using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static
{
    partial class JavaScriptMapOperation
    {

        private JintEngineEx EngineExJint => (JintEngineEx)_engineHandle;
        
        public void SetContextJint()
        {
        }
    }
}
