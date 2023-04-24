using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.System.Processors.CompareExchange;

internal class CompareExchangeHandlerProcessorForGetCompareExchangeValues : AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues<DatabaseRequestHandler, DocumentsOperationContext>
{
    public CompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.Database.CompareExchangeStorage)
    {
    }

    internal class CompareExchangeListItem
    {
        public string Key { get; set; }
        public object Value { get; set; }
        public long Index { get; set; }
    }
}
