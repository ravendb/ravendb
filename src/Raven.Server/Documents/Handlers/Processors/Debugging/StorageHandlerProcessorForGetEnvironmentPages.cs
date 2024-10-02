using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Corax;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Voron.Data.Containers;
using Voron.Data.PostingLists;

namespace Raven.Server.Documents.Handlers.Processors.Debugging;

internal sealed class StorageHandlerProcessorForGetEnvironmentPages : AbstractStorageHandlerProcessorForGetEnvironmentReport<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StorageHandlerProcessorForGetEnvironmentPages([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var type = GetEnvironmentType();
        var details = GetDetails();

        var env = RequestHandler.Database.GetAllStoragesEnvironment()
            .FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) && x.Type == type);

        if (env == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using(var tx = env.Environment.ReadTransaction())
        {
            await using var sw = new StreamWriter( RequestHandler.ResponseBodyStream());
            var owners = env.Environment.GetPageOwners(tx, postingList =>
            {
                if (postingList.Name.ToString() != "LargePostingListsSet")
                    return null;

                var list = new List<long>();
                Span<long> buffer = stackalloc long[1024];
                var it = postingList.Iterate();
                unsafe
                {
                    while (it.Fill(buffer, out var read))
                    {
                        for (int i = 0; i < read; i++)
                        {
                            Container.Get(tx.LowLevelTransaction, buffer[i], out var item);
                            var state = (PostingListState*)item.Address;
                            var pl = new PostingList(tx.LowLevelTransaction, Constants.IndexWriter.LargePostingListsSetSlice, *state);
                            list.AddRange(pl.AllPages());
                        }
                    }
                }

                return list;
            });

            var gaps = new List<(long Start, long End)>();

            long totalPages = tx.LowLevelTransaction.DataPagerState.NumberOfAllocatedPages;
            for (long i = 0; i < totalPages; i++)
            {
                if (owners.ContainsKey(i) == false)
                {
                    var start = i;
                    while (i < totalPages)
                    {
                        if (owners.ContainsKey(i))
                            break;
                        i++;
                    }
                    gaps.Add((start, i));
                }
            }
            
            // This endpoint is here solely for debugging RavenDB itself
            // we use that when we need to figure out discrepancies in the storage report
            // it is not meant for general consumption, that is partly why it returns text
            // and not JSON, this is meant purely to be human readable.
            if (gaps.Count > 0)
            {
                await sw.WriteLineAsync("Gaps");
                await sw.WriteLineAsync("------------------");
                foreach ((long start, long end) in gaps)
                {
                    await sw.WriteLineAsync($"{start}, {end}");
                }
                await sw.WriteLineAsync("------------------");
            }

            await sw.WriteLineAsync("Pages");
            await sw.WriteLineAsync("------------------");

            foreach (var (k,v)  in owners.OrderBy(x=>x.Key))
            {
                await sw.WriteLineAsync($"{k},{v}");
            }
            await sw.WriteLineAsync("------------------");
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
