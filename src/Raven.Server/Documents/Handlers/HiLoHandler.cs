// -----------------------------------------------------------------------
//  <copyright file="HiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client.Documents.Identity;
using Raven.Server.Documents.Handlers.Processors.HiLo;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class HiLoHandler : DatabaseRequestHandler
    {
        public const string RavenHiloIdPrefix = "Raven/Hilo/";


        [RavenAction("/databases/*/hilo/next", "GET", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task GetNextHiLo()
        {
            using (var processor = new HiLoHandlerProcessorForGetNextHiLo(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/hilo/return", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task HiLoReturn()
        {
            var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
            var end = GetLongQueryString("end");
            var last = GetLongQueryString("last");

            var cmd = new MergedHiLoReturnCommand
            {
                Database = Database,
                Key = tag,
                End = end,
                Last = last
            };

            await Database.TxMerger.Enqueue(cmd);

            NoContentStatus();
        }

        internal class MergedHiLoReturnCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public DocumentDatabase Database;
            public long End;
            public long Last;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var hiLoDocumentId = RavenHiloIdPrefix + Key;

                var document = Database.DocumentsStorage.Get(context, hiLoDocumentId);

                if (document == null)
                    return 1;

                document.Data.TryGet(nameof(HiloDocument.Max), out long oldMax);
                if (oldMax != End || Last > oldMax)
                    return 1;

                document.Data.Modifications = new DynamicJsonValue
                {
                    [nameof(HiloDocument.Max)] = Last
                };

                using (var hiloReader = context.ReadObject(document.Data, hiLoDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Database.DocumentsStorage.Put(context, hiLoDocumentId, null, hiloReader);
                }

                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedHiLoReturnCommandDto
                {
                    Key = Key,
                    End = End,
                    Last = Last
                };
            }
        }
    }

    internal class MergedHiLoReturnCommandDto : TransactionOperationsMerger.IReplayableCommandDto<HiLoHandler.MergedHiLoReturnCommand>
    {
        public string Key;
        public long End;
        public long Last;

        public HiLoHandler.MergedHiLoReturnCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new HiLoHandler.MergedHiLoReturnCommand()
            {
                Key = Key,
                End = End,
                Last = Last,
                Database = database
            };
        }
    }
}
