using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static
{
    partial class JavaScriptReduceOperation
    {

        private V8EngineEx EngineExV8 => (V8EngineEx)EngineHandle;
        
        public void SetContextV8()
        {
            EngineExV8.SetContext(_index.ContextExV8);
        }
    }
}
