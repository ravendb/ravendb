using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal partial class SqlDocumentTransformer
    {
        protected override void AddLoadedAttachmentJint(JsValue reference, string name, Attachment attachment)
        {
            var strReference = reference.ToString();
            if (_loadedAttachments.TryGetValue(strReference, out var loadedAttachments) == false)
            {
                loadedAttachments = new Queue<Attachment>();
                _loadedAttachments.Add(strReference, loadedAttachments);
            }

            loadedAttachments.Enqueue(attachment);
        }

        protected override void AddLoadedCounterJint(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by SQL ETL");
        }

        protected override void AddLoadedTimeSeriesJint(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by SQL ETL");
        }

        private JsValue ToVarcharTranslatorJint(JsValue type, JsValue[] args)
        {
            var engineEx = (JintEngineEx)DocumentEngineHandle;
            if (args[0].IsString() == false)
                throw new InvalidOperationException("varchar() / nvarchar(): first argument must be a string");

            var sizeSpecified = args.Length > 1;

            if (sizeSpecified && args[1].IsNumber() == false)
                throw new InvalidOperationException("varchar() / nvarchar(): second argument must be a number");

            var item = engineEx.Realm.Intrinsics.Object.Construct(Arguments.Empty);

            item.Set(nameof(VarcharFunctionCall.Type), type, true);
            item.Set(nameof(VarcharFunctionCall.Value), args[0], true);
            item.Set(nameof(VarcharFunctionCall.Size), sizeSpecified ? args[1] : DefaultVarCharSize, true);

            return item;
        }
    }
}
