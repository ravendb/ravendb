using System;
using System.Collections.Generic;
using Jurassic;
using Raven.Client.ServerWide.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed> : DocumentPatcherBase where TExtracted : ExtractedItem
    {
        protected readonly DocumentsOperationContext Context;

        protected TExtracted Current;

        protected EtlTransformer(DocumentDatabase database, DocumentsOperationContext context) : base(database)
        {
            Context = context;
        }

        protected abstract string[] LoadToDestinations { get; }

        protected override void CustomizeEngine(ScriptEngine engine, PatcherOperationScope scope)
        {
            engine.SetGlobalFunction(Transformation.LoadTo, new Action<string, object>((tableName, colsAsObject) => LoadToFunction(tableName, colsAsObject, scope)));

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < LoadToDestinations.Length; i++)
            {
                var collection = LoadToDestinations[i];
                engine.SetGlobalFunction(Transformation.LoadTo + collection, (Action<object>)(cols =>
                {
                    LoadToFunction(collection, cols, scope);
                }));
            }
        }

        protected abstract void LoadToFunction(string tableName, object colsAsObject, PatcherOperationScope scope);

        public abstract IEnumerable<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item);

        public static void ThrowLoadParameterIsMandatory(string parameterName)
        {
            throw new ArgumentException($"{parameterName} parameter is mandatory");
        }
    }
}
