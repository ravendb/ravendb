using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Corax;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Voron;
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

        Dictionary<long, string> owners;
        long totalPages;

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = env.Environment.ReadTransaction()) 
        {
            totalPages = tx.LowLevelTransaction.DataPagerState.NumberOfAllocatedPages;
            owners = env.Environment.GetPageOwners(tx, postingList =>
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
                            Container.Get(tx.LowLevelTransaction, new ContainerEntryId(buffer[i]), out var item);
                            var state = (PostingListState*)item.Address;
                            var pl = new PostingList(tx.LowLevelTransaction, Constants.IndexWriter.LargePostingListsSetSlice, *state);
                            list.AddRange(pl.AllPages());
                        }
                    }
                }

                return list;
            });
        }

        var gaps = new List<(long Start, long End)>();

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

        var output = RequestHandler.GetEnumQueryString<OutputType>("output", required: false);

        switch (output)
        {
            case OutputType.Emojis:
                await RenderEmojis(env.Environment, owners, gaps, totalPages);
                break;
            default:
                await RenderRaw(env.Environment, owners, gaps);
                break;
        }
    }

    private enum OutputType
    {
        Emojis,
        Text,
    }

    private async Task RenderRaw(StorageEnvironment env, Dictionary<long, string> owners, List<(long Start, long End)> gaps)
    {
        HttpContext.Response.Headers.ContentType = "text/plain; charset=utf-8";

        // This endpoint is here solely for debugging RavenDB itself
        // we use that when we need to figure out discrepancies in the storage report
        // it is not meant for general consumption, that is partly why it returns text
        // and not JSON, this is meant purely to be human readable.
        await using var sw = new StreamWriter(RequestHandler.ResponseBodyStream(), Encoding.UTF8);
        
        if (gaps.Count > 0)
        {
            await sw.WriteLineAsync("Gaps");
            await sw.WriteLineAsync("------------------");
            foreach ((long start, long end) in gaps)
            {
                await sw.WriteLineAsync($"{start}-{end}");
                for (long i = start; i < end; i++)
                {
                    await sw.WriteLineAsync($"  Page {i:N0}");
                }
            }
            await sw.WriteLineAsync("------------------");
            await sw.WriteLineAsync();
        }

        await sw.WriteLineAsync("Pages");
        await sw.WriteLineAsync("------------------");

        foreach (var (k, v) in owners.OrderBy(x => x.Key))
        {
            await sw.WriteLineAsync($"{k},{v}");
        }
        await sw.WriteLineAsync("------------------");
    }

    private async Task RenderEmojis(StorageEnvironment env, Dictionary<long, string> owners, List<(long Start, long End)> gaps, long totalPages)
    {
        await using var sw = new StreamWriter(RequestHandler.ResponseBodyStream(), Encoding.UTF8);
        HttpContext.Response.Headers.ContentType = "text/html; charset=utf-8";

        var filePath = env.DataPager.FileName;
        
        var pages = new string[totalPages];
        Array.Fill(pages, "Unassigned");
        
        foreach (var gap in gaps)
        {
            for (long i = gap.Start; i < gap.End && i < totalPages; i++)
                pages[i] = "Gap";
        }

        foreach (var (page, owner) in owners)
        {
            if (page < totalPages)
                pages[page] = owner;
        }

        var sparse = env.DataPager.GetSparsePages(env.CurrentStateRecord.DataPagerState);
        foreach (var page in sparse)
        {
            if (page >= totalPages)
                continue; // race betwee starting this check and the file growing, probably

            if (pages[page] != "Freed Page")
            {
                pages[page] = "SPARSE!!! AND " + pages[page];
            }
            else
            {
                pages[page] = "Sparse";
            }
        }

        var usedOwners = new Dictionary<string, (string Emoji, string Description)>();
        foreach (var page in pages)
        {
            if (usedOwners.ContainsKey(page) == false)
            {
                usedOwners[page] = GetEmojiAndDescription(page);
            }
        }

        await sw.WriteAsync("""
            <!DOCTYPE html>
            <html>
            <head>
            <meta charset='utf-8'>
            <title>Voron Storage Details</title>
            <style>
            body { font-family: 'Courier New', monospace; }
            .legend { margin-bottom: 20px; }
            .page-line { white-space: nowrap; margin: 2px 0; }
            .page-num { display: inline-block; width: 100px; color: #858585; }
            </style>
            </head>
            <body>
            <div class='legend'>
            <h3>Legend:</h3>
            """);
        
        foreach (var (emoji, description) in usedOwners.OrderBy(x => x.Value.Description)
            .GroupBy(x => x.Value)
            .Select(g => g.Key))
        {
            await sw.WriteLineAsync($"<div>{emoji} = {description}</div>");
        }
        await sw.WriteLineAsync("</div>");

        long gapCount = 0;
        foreach (var gap in gaps)
            gapCount += gap.End - gap.Start;

        await sw.WriteLineAsync($"<div>Total: {totalPages:N0} pages ({new Sparrow.Size(totalPages * 8, Sparrow.SizeUnit.Kilobytes)}) | Gaps: {gapCount:N0} pages ({new Sparrow.Size(gapCount * 8, Sparrow.SizeUnit.Kilobytes)})</div>");
        await sw.WriteLineAsync("<br/>");

        for (long i = 0; i < totalPages; i += 128)
        {
            await sw.WriteAsync("<div class='page-line'>");
            await sw.WriteAsync($"<span class='page-num'>{i,8:#,#}</span>");
            await sw.WriteAsync("<span>|</span>");
            
            for (int j = 0; j < 128 && i + j < totalPages; )
            {
                var currentOwner = pages[i + j];
                var (emoji, _) = usedOwners[currentOwner];
                
                // Count consecutive pages with the same owner
                int count = 1;
                while (j + count < 128 && i + j + count < totalPages && pages[i + j + count] == currentOwner)
                    count++;
                
                await sw.WriteAsync($"<span title='{currentOwner}'>");
                
                for (int k = 0; k < count; k++)
                {
                    await sw.WriteAsync(emoji);
                }
                
                await sw.WriteAsync("</span>");
                
                j += count;
            }
            
            await sw.WriteLineAsync("</div>");
        }
        
        await sw.WriteLineAsync("</body>");
        await sw.WriteLineAsync("</html>");
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    private static (string Emoji, string Description) GetEmojiAndDescription(string owner)
    {
        if(owner.StartsWith("SPARSE!!! AND "))
            return ("🚨", "Sparse DUPE (BUG!)");
        if (owner == "Gap")
            return ("🚨", "Gap (BUG!)");
        if (owner == "Sparse")
            return ("🗑️", "Sparse (returned to OS...)");
        if (owner == "Unassigned")
            return ("⬜", "Unassigned");
        if (owner == "Freed Page")
            return ("🆓", "Freed Page");
        if (owner == "Unused Page")
            return ("⚪", "Unused Page");
        if (owner == "$free-space")
            return ("♻️", "$free-space");
        if (owner.StartsWith("Collection.Documents."))
            return ("📄", "Documents");
        if (owner.StartsWith("Collection.Tombstones."))
            return ("🪦", "Tombstones");
        if (owner.StartsWith("Collection.Revisions."))
            return ("📜", "Revisions");
        if (owner.StartsWith("Collection.TimeSeries."))
            return ("📈", "TimeSeries");
        if (owner.StartsWith("Collection.TimeSeriesStats."))
            return ("📊", "TimeSeriesStats");
        if (owner.StartsWith("Collection.CounterGroups."))
            return ("🔢", "Counters");
        if (owner.StartsWith("Collection.Attachments."))
            return ("🗂️", "Attachments");
        if (owner.StartsWith("AttachmentsMetadata"))
            return ("🗃️", "AttachmentsMetadata");
        if (owner.StartsWith("Attachments"))
            return ("📎", "Attachments");
        if (owner.StartsWith("Collections"))
            return ("📚", "Collections");
        if (owner.StartsWith("Conflicts"))
            return ("⚠️", "Conflicts");
        if (owner.StartsWith("Revisions"))
            return ("📖", "Revisions");
        if (owner.StartsWith("Tombstones"))
            return ("💀", "Tombstones");
        if (owner.StartsWith("TimeSeries"))
            return ("⏱️", "TimeSeries");
        if (owner.Contains("Metadata") || owner.Contains("(VST)"))
            return ("🔧", owner);
        return ("🟦", owner);
    }

}
