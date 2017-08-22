using System;
using System.Collections.Generic;
using Jurassic;
using Raven.Client.ServerWide.ETL;
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
            _returnRun = Database.Scripts.GetScriptRunner(_key, out SingleRun);
            if (SingleRun == null)
                return;
            SingleRun.ReadOnly = true;
            SingleRun.ScriptEngine.SetGlobalFunction(Transformation.LoadTo, (Action<string, object>)LoadToFunction);
            for (var i = 0; i < LoadToDestinations.Length; i++)
            {
                var collection = LoadToDestinations[i];
                SingleRun.ScriptEngine.SetGlobalFunction(Transformation.LoadTo + collection, (Action<object>)(cols =>
                {
                    LoadToFunction(collection, cols);
                }));
            }
        }

        protected abstract string[] LoadToDestinations { get; }

        protected abstract void LoadToFunction(string tableName, object colsAsObject);

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
