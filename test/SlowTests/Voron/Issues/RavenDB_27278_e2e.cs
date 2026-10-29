using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Util;
using Xunit;

namespace SlowTests.Voron.Issues;

// A corrupted transaction belonging to ONE index in the shared
// journals marks only that index as faulty (State reads Normal but Type is Faulty) after a database
// reload - sibling indexes and documents are unaffected and the database loads.
//
// To inspect the result in the Studio, run under a debugger - it pauses at WaitForUserToContinueTheTest
// after the corruption + reload. To see the pre-fix blast radius (sibling indexes faulty too), revert
// the resume-on-invalid changes in JournalReader.cs and swap the commented assert at the end.
public class RavenDB_27278_e2e(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private static IndexDefinition MapIndex(string name) => new()
    {
        Name = name,
        Maps = { "from i in docs.Items select new { i.Name, i.Value }" }
    };

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task CorruptedTxOfSingleIndex_MarksSiblingIndexesAsFaulty()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r =>
            {
                // production default (the test infrastructure overrides it to true): index open failures
                // produce faulty indexes instead of failing the whole database load
                r.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
            }
        });

        foreach (var n in new[] { "Idx/A", "Idx/B", "Idx/C" })
            await store.Maintenance.SendAsync(new PutIndexesOperation(MapIndex(n)));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.NotNull(database.IndexStore.SharedJournals);

        // block syncing before any data is indexed: a synced environment starts its replay past the
        // corruption point and masks the scenario
        database.IndexStore.SharedJournals.Env.Options.ManualSyncing = true;
        foreach (var idx in database.IndexStore.GetIndexes())
            idx._environment.Options.ManualSyncing = true;

        Guid rootId = database.IndexStore.SharedJournals.Env.HeaderAccessor.JournalId;
        var indexes = database.IndexStore.GetIndexes()
            .Select(idx => (idx.Name, Id: idx._environment.HeaderAccessor.JournalId, Journals: Path.Combine(idx._environment.Options.BasePath.FullPath, "Journals")))
            .ToList();

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 500; i++)
                await bulk.StoreAsync(new Item { Name = $"item-{i}", Value = i }, $"items/{i}");
        }
        Indexes.WaitForIndexing(store);

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));

        // the victim: a transaction picked from the owner's OWN journal chain (an environment replays only
        // the journals in its own dir, so the owner is guaranteed to hit it), with a later own transaction
        // (an owner with none just truncates there) and no later ROOT transaction (the root failing to
        // open fails the WHOLE database load, not just indexes)
        string victimOwner = null, journalFile = null;
        (long Offset, Guid JournalId, long TxId) victim = default;
        foreach (var index in indexes)
        {
            // the toggle returns before the unloaded database releases its file handles
            await WaitForExclusiveJournalAccessAsync(index.Journals);

            foreach (string file in Directory.GetFiles(index.Journals, "*.journal").OrderByDescending(f => f))
            {
                List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(await File.ReadAllBytesAsync(file));
                victim = txs.FirstOrDefault(t =>
                    t.JournalId == index.Id &&
                    txs.Any(l => l.Offset > t.Offset && l.JournalId == index.Id) &&
                    txs.Any(l => l.Offset > t.Offset && l.JournalId == rootId) == false);
                if (victim != default)
                {
                    (victimOwner, journalFile) = (index.Name, file);
                    break;
                }
            }

            if (journalFile != null)
                break;
        }

        Assert.True(journalFile != null, "no journal contains an index transaction followed by another transaction of the same index and no root transaction");
        Output.WriteLine($"corrupting tx {victim.TxId} of '{victimOwner}' at offset {victim.Offset} in {Path.GetFileName(journalFile)}");

        // flip one payload byte: the header stays readable, only the hash validation fails. The file is
        // hard-linked, so every environment sharing it sees the corruption
        using (var fs = new FileStream(journalFile, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Position = victim.Offset + TransactionHeader.SizeOf;
            int payloadByte = fs.ReadByte();
            fs.Position = victim.Offset + TransactionHeader.SizeOf;
            fs.WriteByte((byte)(payloadByte ^ 0xFF));
        }

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));

        // the database loads and documents are intact
        long docCount;
        using (var session = store.OpenAsyncSession())
            docCount = await session.Query<Item>().CountAsync();
        Assert.Equal(500, docCount);

        var stats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
        foreach (var s in stats)
            Output.WriteLine($"{s.Name}: State={s.State}, Type={s.Type}" + (s.Name == victimOwner ? "   <- owner of the corrupted tx" : string.Empty));

        // run under a debugger to inspect the indexes in the Studio at this point
        WaitForUserToContinueTheTest(store);

        var faulty = stats.Where(s => s.Type == IndexType.Faulty).Select(s => s.Name).ToList();

        // with the fix: only the owner of the corrupted transaction faults
        Assert.Equal(new[] { victimOwner }, faulty);

        // pre-fix blast radius (revert the resume-on-invalid changes in JournalReader.cs to see it):
        // siblings get faulty too
        //Assert.True(faulty.Count(name => name != victimOwner) >= 1,
        //    $"expected sibling indexes to be faulty although only one transaction of '{victimOwner}' was corrupted, but faulty are: [{string.Join(", ", faulty)}]");
    }

    private static async Task WaitForExclusiveJournalAccessAsync(string journalsDir)
    {
        foreach (string file in Directory.GetFiles(journalsDir, "*.journal"))
        {
            for (int i = 0; ; i++)
            {
                try
                {
                    using (new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                        break;
                }
                catch (IOException) when (i < 100)
                {
                    await Task.Delay(100);
                }
            }
        }
    }

    private static unsafe List<(long Offset, Guid JournalId, long TxId)> ReadTransactions(byte[] journal)
    {
        var txs = new List<(long, Guid, long)>();
        fixed (byte* p = journal)
        {
            var incarnation = Guid.Empty;
            long pos = 0;
            while (pos + TransactionHeader.SizeOf <= journal.Length)
            {
                var header = (TransactionHeader*)(p + pos);
                if (header->HeaderMarker != Constants.TransactionHeaderMarker)
                {
                    pos += 4 * 1024;
                    continue;
                }

                if ((header->Flags & TransactionPersistenceModeFlags.JournalHeaderRecord) != 0)
                {
                    // RavenDB-27397: every journal opens with a header record whose payload is the
                    // incarnation that the JournalId of all subsequent entries is XORed with
                    incarnation = *(Guid*)((byte*)header + TransactionHeader.SizeOf);
                    pos += 4 * 1024;
                    continue;
                }

                txs.Add((pos, header->JournalId.Xor(incarnation), header->TransactionId));

                long size = header->CompressedSize != -1 ? header->CompressedSize : header->UncompressedSize;
                long sizeIn4Kb = (size + sizeof(TransactionHeader)) / (4 * 1024) +
                                 ((size + sizeof(TransactionHeader)) % (4 * 1024) == 0 ? 0 : 1); // JournalReader.GetTransactionSizeIn4Kb
                pos += sizeIn4Kb * 4 * 1024;
            }
        }

        return txs;
    }
}
