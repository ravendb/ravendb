using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Counters
{
    internal abstract class AbstractCountersHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractCountersHandlerProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        internal static void GetCounterValue(DocumentsOperationContext context, DocumentDatabase database, string docId,
            string counterName, bool addFullValues, CountersDetail result, bool capValueOnOverflow = false)
        {
            long value = 0;
            long etag = 0;
            result.Counters ??= new List<CounterDetail>();
            Dictionary<string, long> fullValues = null;

            if (addFullValues)
            {
                fullValues = new Dictionary<string, long>();
                foreach (var partialValue in database.DocumentsStorage.CountersStorage.GetCounterPartialValues(context, docId, counterName))
                {
                    etag = HashCode.Combine(etag, partialValue.Etag);
                    try
                    {
                        value = checked(value + partialValue.PartialValue);
                    }
                    catch (OverflowException e)
                    {
                        if (capValueOnOverflow == false)
                            CounterOverflowException.ThrowFor(docId, counterName, e);

                        value = value + partialValue.PartialValue > 0 ?
                            long.MinValue :
                            long.MaxValue;
                    }

                    fullValues[partialValue.ChangeVector] = partialValue.PartialValue;
                }

                if (fullValues.Count == 0)
                {
                    result.Counters.Add(null);
                    return;
                }
            }
            else
            {
                var v = database.DocumentsStorage.CountersStorage.GetCounterValue(context, docId, counterName, capValueOnOverflow);

                if (v == null)
                {
                    result.Counters.Add(null);
                    return;
                }

                value = v.Value.Value;
                etag = v.Value.Etag;
            }

            result.Counters.Add(new CounterDetail
            {
                DocumentId = docId,
                CounterName = counterName,
                TotalValue = value,
                CounterValues = fullValues,
                Etag = etag
            });
        }
    }
}
