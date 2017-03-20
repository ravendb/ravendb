using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed> : DocumentPatcherBase
    {
        protected const string LoadTo = "loadTo";

        protected readonly DocumentsOperationContext Context;

        protected string[] LoadToDestinations = new string[0];

        protected EtlTransformer(DocumentDatabase database, DocumentsOperationContext context) : base(database)
        {
            Context = context;
        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            base.RemoveEngineCustomizations(engine, scope);

            engine.Global.Delete(LoadTo, true);

            foreach (var destination in LoadToDestinations)
            {
                engine.Global.Delete(LoadTo + destination, true);
            }
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            base.CustomizeEngine(engine, scope);

            engine.SetValue(LoadTo, new Action<string, JsValue>((tableName, colsAsObject) => LoadToFunction(tableName, colsAsObject, scope)));

            foreach (var destination in LoadToDestinations)
            {
                engine.SetValue(LoadTo + destination, (Action<JsValue>)(cols =>
                {
                    LoadToFunction(destination, cols, scope);
                }));
            }
        }

        protected abstract void LoadToFunction(string tableName, JsValue colsAsObject, PatcherOperationScope scope);

        public abstract IEnumerable<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item);
    }
}