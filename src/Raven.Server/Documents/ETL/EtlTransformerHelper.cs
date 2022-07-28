using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.ETL;

public static class EtlTransformerHelper
{
    public static ScriptRunnerResultV8 GetScriptRunnerResultV8(object obj, SingleRun<JsHandleV8> documentScript, IJsEngineHandle<JsHandleV8> documentEngineHandle)
    {
        if (obj is JsHandleV8 jsHandle)
        {
            return new ScriptRunnerResultV8(documentScript, jsHandle);
        }

        return new ScriptRunnerResultV8(documentScript, documentEngineHandle.FromObjectGen(obj, keepAlive: false)); //TODO: egor true/false?
    }

    public static ScriptRunnerResultJint GetScriptRunnerResultJint(object obj, SingleRun<JsHandleJint> documentScript, IJsEngineHandle<JsHandleJint> documentEngineHandle)
    {
        if (obj is JsHandleJint jsHandle)
        {
            return new ScriptRunnerResultJint(documentScript, jsHandle);
        }

        return new ScriptRunnerResultJint(documentScript, documentEngineHandle.FromObjectGen(obj, keepAlive: false)); //TODO: egor true/false?
    }

    public static ReturnRun CreateDocumentScriptRunnerV8(DocumentDatabase database, PatchRequest mainScript, bool debugMode, out SingleRun<JsHandleV8> documentScript)
    {
        var returnRun = database.Scripts.GetScriptRunnerV8(mainScript, readOnly: true, out documentScript);
        if (documentScript != null)
            documentScript.DebugMode = debugMode;

        return returnRun;
    }

    public static ReturnRun CreateDocumentScriptRunnerJint(DocumentDatabase database, PatchRequest mainScript, bool debugMode,
        out SingleRun<JsHandleJint> documentScript)
    {
        var returnRun = database.Scripts.GetScriptRunnerJint(mainScript, readOnly: true, out documentScript);
        if (documentScript != null)
            documentScript.DebugMode = debugMode;
  
        return returnRun;
    }
}
