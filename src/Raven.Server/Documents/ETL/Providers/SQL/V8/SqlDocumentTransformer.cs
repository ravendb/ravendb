using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.V8;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal partial class SqlDocumentTransformer
    {
        protected override void AddLoadedAttachmentV8(InternalHandle reference, string name, Attachment attachment)
        {
            var strReference = reference.ToString();
            if (_loadedAttachments.TryGetValue(strReference, out var loadedAttachments) == false)
            {
                loadedAttachments = new Queue<Attachment>();
                _loadedAttachments.Add(strReference, loadedAttachments);
            }

            loadedAttachments.Enqueue(attachment);
        }

        protected override void AddLoadedCounterV8(InternalHandle reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by SQL ETL");
        }

        protected override void AddLoadedTimeSeriesV8(InternalHandle reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by SQL ETL");
        }

        private InternalHandle ToVarcharTranslatorV8(string type, InternalHandle[] args)
        {
            if (args[0].IsStringEx == false)
                throw new InvalidOperationException("varchar() / nvarchar(): first argument must be a string");

            var sizeSpecified = args.Length > 1;

            if (sizeSpecified && args[1].IsInt32 == false)
                throw new InvalidOperationException("varchar() / nvarchar(): second argument must be an integer");

            InternalHandle item = DocumentEngineV8.CreateObject();
            {
                if (item.SetProperty(nameof(VarcharFunctionCall.Type), DocumentEngineV8Ex.CreateValue(type)) == false)
                    throw new InvalidOperationException($"Failed to set {nameof(VarcharFunctionCall.Type)} on item");
                if (item.SetProperty(nameof(VarcharFunctionCall.Value), new InternalHandle(ref args[0], true)) == false)
                    throw new InvalidOperationException($"Failed to set {nameof(VarcharFunctionCall.Value)} on item");
                if (item.SetProperty(nameof(VarcharFunctionCall.Size), sizeSpecified ? new InternalHandle(ref args[1], true) : DocumentEngineV8Ex.CreateValue(DefaultVarCharSize)) == false)
                    throw new InvalidOperationException($"Failed to set {nameof(VarcharFunctionCall.Size)} on item");
            }
            return item;
        }
    }
}
