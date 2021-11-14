using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static
{
    partial class JavaScriptReduceOperation
    {

        private JintEngineEx EngineExJint => (JintEngineEx)EngineHandle;
        
        public void SetContextJint()
        {
        }
    }
}
