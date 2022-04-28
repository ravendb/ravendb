using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Counters
{
    internal class CountersHandlerProcessorForGetCounters : AbstractCountersHandlerProcessorForGetCounters<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CountersHandlerProcessorForGetCounters([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<CountersDetail> GetCountersAsync(DocumentsOperationContext context, string docId, StringValues counters, bool full)
        {
            using (context.OpenReadTransaction())
                return ValueTask.FromResult(GetInternal(RequestHandler.Database, context, counters, docId, full));
        }

        internal static CountersDetail GetInternal(DocumentDatabase database, DocumentsOperationContext context, StringValues counters, string docId, bool full)
        {
            var result = new CountersDetail();
            var names = counters.Count != 0
                ? counters
                : database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, docId);

            foreach (var counter in names)
            {
                GetCounterValue(context, database, docId, counter, full, result);
            }

            return result;
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
