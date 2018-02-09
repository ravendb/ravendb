using System;
using System.Collections.Generic;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed> : IDisposable  where TExtracted : ExtractedItem
    {
        public DocumentDatabase Database { get; }
        protected readonly DocumentsOperationContext Context;
        private readonly ScriptRunnerCache.Key _key;
        protected ScriptRunner.SingleRun SingleRun;

        protected TExtracted Current;
        private ScriptRunner.ReturnRun _returnRun;

        protected EtlTransformer(DocumentDatabase database, DocumentsOperationContext context,
            ScriptRunnerCache.Key key)
        {
            Database = database;
            Context = context;
            _key = key;
        }

        public virtual void Initalize()
        {
            _returnRun = Database.Scripts.GetScriptRunner(_key, true, out SingleRun);
            if (SingleRun == null)
                return;
            SingleRun.ScriptEngine.SetValue(Transformation.LoadTo, new ClrFunctionInstance(SingleRun.ScriptEngine,LoadToFunctionTranslator));
            
            SingleRun.ScriptEngine.SetValue(Transformation.LoadAttachment, new ClrFunctionInstance(SingleRun.ScriptEngine,LoadAttachment));

            for (var i = 0; i < LoadToDestinations.Length; i++)
            {
                var collection = LoadToDestinations[i];
                var clrFunctionInstance = new ClrFunctionInstance(SingleRun.ScriptEngine,(value, values) => LoadToFunctionTranslator(collection, value, values));
                SingleRun.ScriptEngine.SetValue(Transformation.LoadTo + collection, clrFunctionInstance);
            }
        }

        private JsValue LoadAttachment(JsValue self, JsValue[] args)
        {
            if(args.Length != 1 || args[0].IsString() == false)
                throw new InvalidOperationException("loadAttachment(name) must have a single string argument");
            
            return new JsValue(Transformation.AttachmentMarker + args[0].AsString());
        }

        private JsValue LoadToFunctionTranslator(JsValue self, JsValue[] args)
        {
            if(args.Length != 2)
                throw new InvalidOperationException("loadTo(name, obj) must be called with exactly 2 parameters");
            
            if(args[0].IsString() == false)
                throw new InvalidOperationException("loadTo(name, obj) first argument must be a string");
            if(args[1].IsObject() == false)
                throw new InvalidOperationException("loadTo(name, obj) second argument must be an object");


            using (var result = new ScriptRunnerResult(SingleRun, args[1].AsObject()))
                LoadToFunction(args[0].AsString(), result);
            
            return self;
        }
        
        private JsValue LoadToFunctionTranslator(string name, JsValue self, JsValue[] args)
        {
            if(args.Length != 1)
                throw new InvalidOperationException($"loadTo{name}(, obj) must be called with exactly 1 parameter");
            
            if(args[0].IsObject() == false)
                throw new InvalidOperationException($"loadTo{name} argument must be an object");


            using (var result = new ScriptRunnerResult(SingleRun, args[0].AsObject()))
                LoadToFunction(name, result);
            
            return self;
        }

        protected abstract string[] LoadToDestinations { get; }

        protected abstract void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject);

        public abstract IEnumerable<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item);

        public static void ThrowLoadParameterIsMandatory(string parameterName)
        {
            throw new ArgumentException($"{parameterName} parameter is mandatory");
        }

        public void Dispose()
        {
            _returnRun.Dispose();
        }
    }
}
