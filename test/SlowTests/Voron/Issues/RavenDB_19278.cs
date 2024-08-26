using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests.Voron;
using Sparrow.Platform;
using Sparrow.Utils;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_19278 : StorageTest
{
    private List<string> _onRecoveryErrorMessages = new();

    private bool _encrypted;

    public RavenDB_19278(ITestOutputHelper output) : base(output)
    {
    }

    private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.InitialLogFileSize = 4 * Constants.Size.Megabyte;
        options.MaxLogFileSize = 32 * Constants.Size.Megabyte;

        options.OnRecoveryError += (sender, args) =>
        {
            _onRecoveryErrorMessages.Add(args.Message);
        };
        options.ManualSyncing = true;
        options.ManualFlushing = true;
        options.MaxScratchBufferSize = 1 * Constants.Size.Gigabyte;

        if (_encrypted)
            options.Encryption.MasterKey = _masterKey.ToArray();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StopRecoveryAndRaisePartiallyRecoveredAlertAfterGettingInvalidHashOfTransaction(bool encrypted)
    {
        _encrypted = encrypted;

        RequireFileBasedPager();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("tree");

            tx.Commit();
        }

        const long corruptedTx = 9;

        var random = new Random(3);

        var itemsToReadAfterRecovery = new List<string>();

        for (var i = 0; i < 10; i++)
        {
            var buffer = new byte[2 * Constants.Size.Megabyte];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                for (int j = 0; j < 10; j++)
                {
                    string key = "a" + i + j;
                    tx.CreateTree("tree").Add(key, new MemoryStream(buffer));

                    if (tx.LowLevelTransaction.Id < corruptedTx)
                    {
                        itemsToReadAfterRecovery.Add(key);
                    }
                }

                tx.Commit();
            }
        }

        var journalToCorrupt = Env.Journal.GetCurrentJournalInfo().CurrentJournal - 3;

        StopDatabase();

        CorruptJournal(journalToCorrupt, 10 * Constants.Size.Kilobyte * 4 - 1000); // it will corrupt tx 9

        StartDatabase(); // it must not throw, it should partially recover the database

        Assert.Equal(2, _onRecoveryErrorMessages.Count);

        string message;

        if (encrypted == false)
            message = $"Invalid hash signature for transaction: HeaderMarker: Valid, TransactionId: {corruptedTx}";
        else
            message = $"Could not decrypt transaction {corruptedTx}. It could be not committed";

        Assert.Contains(message, _onRecoveryErrorMessages[0]);
        Assert.Contains("Database recovered partially. Some data was lost.", _onRecoveryErrorMessages[1]);

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            operation.SyncDataFile(); // force the sync so it will delete already synced journals
        }

        var journalPath = Env.Options.JournalPath.FullPath;

        // older journals should be deleted, newer but not processed should be deleted too due to partial recovery 
        Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"*.journal").Length == 1,
            TimeSpan.FromSeconds(30)));

        using (var tx = Env.ReadTransaction())
        {
            foreach (var key in itemsToReadAfterRecovery)
            {
                var readA = tx.ReadTree("tree").Read(key);

                Assert.NotNull(readA);
            }
        }

        using (var tx = Env.WriteTransaction())
        {
            Assert.Equal(corruptedTx, tx.LowLevelTransaction.Id);

            var buffer = new byte[123];
            random.NextBytes(buffer);

            tx.CreateTree("tree").Add("new", new MemoryStream(buffer));

            tx.Commit();
        }

        StopDatabase();

        Configure(Options); // when we stop db we always call _options.NullifyHandlers() so we need to hook them up again

        StartDatabase();
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(false, false, true)]
    [InlineData(true, true, true)]
    public void PartialRecoveryMustUpdateEnvironmentHeaderAndEraseCorruptedDataInJournal(bool syncAfterRestart, bool limitMaxJournalSizeAfterRestart, bool encrypted)
    {
        _encrypted = encrypted;

        RequireFileBasedPager();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("tree");

            tx.Commit();
        }

        const long corruptedTx = 9;

        var random = new Random(3);

        var itemsToReadAfterRecovery = new List<string>();

        for (var i = 0; i < 10; i++)
        {
            var buffer = new byte[2 * Constants.Size.Megabyte];
            random.NextBytes(buffer);

            byte[] smallBuffer = new byte[1337];

            using (var tx = Env.WriteTransaction())
            {
                if (tx.LowLevelTransaction.Id is corruptedTx + 1 or corruptedTx + 2)
                {
                    random.NextBytes(smallBuffer);
                    buffer = smallBuffer;
                }

                for (int j = 0; j < 10; j++)
                {
                    string key = "a" + i + j;
                    tx.CreateTree("tree").Add(key, new MemoryStream(buffer));

                    if (tx.LowLevelTransaction.Id < corruptedTx)
                    {
                        itemsToReadAfterRecovery.Add(key);
                    }
                }

                tx.Commit();
            }
        }

        var journalToCorrupt = Env.Journal.GetCurrentJournalInfo().CurrentJournal - 1;

        StopDatabase();

        CorruptJournal(journalToCorrupt, 10 * Constants.Size.Kilobyte * 4 - 1000); // it will corrupt tx 9

        if (limitMaxJournalSizeAfterRestart)
        {
            // this will force deletion of journals due to their size
            Options.MaxLogFileSize = 4 * Constants.Size.Megabyte;
        }

        StartDatabase(); // it must not throw, it should partially recover the database

        Assert.Equal(2, _onRecoveryErrorMessages.Count);

        string message;

        if (encrypted == false)
            message = $"Invalid hash signature for transaction: HeaderMarker: Valid, TransactionId: {corruptedTx}";
        else
            message = $"Could not decrypt transaction {corruptedTx}. It could be not committed";

        Assert.Contains(message, _onRecoveryErrorMessages[0]);
        Assert.Contains("Database recovered partially. Some data was lost.", _onRecoveryErrorMessages[1]);

        if (syncAfterRestart)
        {
            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile(); // force the sync so it will delete already synced journals
            }

            var journalPath = Env.Options.JournalPath.FullPath;

            // older journals should be deleted, newer but not processed should be deleted too due to partial recovery 
            Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"*.journal").Length == 1,
                TimeSpan.FromSeconds(30)));
        }

        using (var tx = Env.ReadTransaction())
        {
            foreach (var key in itemsToReadAfterRecovery)
            {
                var readA = tx.ReadTree("tree").Read(key);

                Assert.NotNull(readA);
            }
        }

        for (int i = 0; i < 4; i++)
        {
            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(corruptedTx + i, tx.LowLevelTransaction.Id);

                var buffer = new byte[123];
                random.NextBytes(buffer);

                tx.CreateTree("tree").Add("new", new MemoryStream(buffer));

                tx.Commit();
            }

            StopDatabase();

            Configure(Options); // when we stop db we always call _options.NullifyHandlers() so we need to hook them up again

            if (syncAfterRestart)
            {
                using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                {
                    operation.SyncDataFile(); // force the sync so it will delete already synced journals
                }
            }

            StartDatabase();
        }
    }

    private void CorruptJournal(long journal, long position, int numberOfCorruptedBytes = Constants.Size.Kilobyte * 4, byte value = 42, bool preserveValue = false)
    {
        Options.Dispose();
        Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        Configure(Options);
        using (var fileStream = SafeFileStream.Create(Options.GetJournalPath(journal).FullPath,
                   FileMode.Open,
                   FileAccess.ReadWrite,
                   FileShare.ReadWrite | FileShare.Delete))
        {
            fileStream.Position = position;

            var buffer = new byte[numberOfCorruptedBytes];

            var remaining = buffer.Length;
            var start = 0;
            while (remaining > 0)
            {
                var read = fileStream.Read(buffer, start, remaining);
                if (read == 0)
                    break;
                start += read;
                remaining -= read;
            }

            for (int i = 0; i < buffer.Length; i++)
            {
                if (buffer[i] != value || preserveValue)
                    buffer[i] = value;
                else
                    buffer[i] = (byte)(value + 1); // we really want to change the original value here so it must not stay the same
            }
            fileStream.Position = position;
            fileStream.Write(buffer, 0, buffer.Length);
        }
    }
}
