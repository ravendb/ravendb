using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Platform;
using Voron;
using Voron.Data.Tables;
using Voron.Global;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.NextGenPagers;

public class BasicNextGen : StorageTest
{
    public BasicNextGen(ITestOutputHelper output) : base(output)
    {
    }

    private unsafe static Span<byte> AsSpan(Page p) => new Span<byte>(p.Pointer, Constants.Storage.PageSize);

    [Fact]
    public void WithAsyncCommit()
    {
        Options.ManualFlushing = true;
        long pageId;
        using (var tx2 = Env.WriteTransaction())
        {
            // force growth of the file
            tx2.LowLevelTransaction.AllocatePage(1024);
            Page allocatePage = tx2.LowLevelTransaction.AllocatePage(1);
            pageId = allocatePage.PageNumber;
            tx2.Commit();
        }
        using (var tx3 = Env.WriteTransaction())
        {
            using Transaction tx4 = tx3.BeginAsyncCommitAndStartNewTransaction(new TransactionPersistentContext());

            Task flushTask = Task.Run(Env.FlushLogToDataFile);

            while (Env.Journal.Applicator.HasUpdateJournalStateAfterFlush == false)
            {
                Thread.Sleep(100);
            }
            
            using (tx3)
            {
                tx3.EndAsyncCommit();
            }

            flushTask.Wait(100);

            tx4.LowLevelTransaction.GetPage(pageId);

            using Transaction tx5 = tx4.BeginAsyncCommitAndStartNewTransaction(new TransactionPersistentContext());
            using (tx4)
            {
                tx4.EndAsyncCommit();
            }

            tx5.LowLevelTransaction.GetPage(pageId);
            
            tx5.Commit();
        }
    }
    
    
    [Fact]
    public void EncryptedStorageAnd_Flush()
    {
        RequireFileBasedPager();
        Options.Encryption.MasterKey = Sodium.GenerateRandomBuffer(32);
        Options.ManualFlushing = true;
        
        Env.FlushLogToDataFile(); // flush pages from db init
       
        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.RootObjects.Add("hi", "there");
            tx.Commit();
        }
    }

    
    [Fact]
    public void EncryptedStorage()
    {
        RequireFileBasedPager();
        Options.Encryption.MasterKey = Sodium.GenerateRandomBuffer(32);
        Options.ManualFlushing = true;
       
        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.RootObjects.Add("hi", "there");
            tx.Commit();
        }
    }

    [Fact]
    public unsafe void CanHandleUpdatesAndFlushing()
    {
        Options.ManualFlushing = true;
        List<(long, int)> pagesAndVals = new();
        for (int j = 0; j < 3; j++)
        {
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < 3; i++)
                {
                    var page = tx.LowLevelTransaction.AllocatePage(1);
                    byte b = (byte)(i+j+2);
                    AsSpan(page)[PageHeader.SizeOf..].Fill(b);
                    
                    pagesAndVals.Add((page.PageNumber,b));
                }
                tx.Commit();
            }
            Env.FlushLogToDataFile();
        }
        
        using (var tx = Env.WriteTransaction())
        {
            foreach (var (p, v) in pagesAndVals)
            {
                Page page = tx.LowLevelTransaction.GetPage(p);
                Assert.Equal(v, *page.DataPointer);
            }
            tx.Commit();
        }
    }
    
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public unsafe void CanHandleRollingBackTx(bool flushManually)
    {
        Options.ManualFlushing = true;
        long pageNum1, pageNum2;
        using (var tx = Env.WriteTransaction())
        {
            var page = tx.LowLevelTransaction.AllocatePage(1);
            pageNum1 = page.PageNumber;
            AsSpan(page)[PageHeader.SizeOf..].Fill(3);
            
            page = tx.LowLevelTransaction.AllocatePage(1);
            pageNum2 = page.PageNumber;
            AsSpan(page)[PageHeader.SizeOf..].Fill(5);

            tx.Commit();
        }
        
        if(flushManually)
            Env.FlushLogToDataFile();
        
        using (var tx = Env.WriteTransaction())
        {
            var page = tx.LowLevelTransaction.ModifyPage(pageNum1);
            AsSpan(page)[PageHeader.SizeOf..].Fill(4);
           // tx.LowLevelTransaction.ModifyPage(pageNum2);
            tx.LowLevelTransaction.FreePage(pageNum2);
            page = tx.LowLevelTransaction.AllocatePage(1);
            Assert.Equal(pageNum2, page.PageNumber);
            AsSpan(page)[PageHeader.SizeOf..].Fill(7);
            using (var rtx = Env.ReadTransaction())
            {
                var readPage = rtx.LowLevelTransaction.GetPage(pageNum1);
                Assert.Equal(3, *readPage.DataPointer);
                readPage = rtx.LowLevelTransaction.GetPage(pageNum2);
                Assert.Equal(5, *readPage.DataPointer);
            }
            // implicit rollback here!
        }
           
        if(flushManually)
            Env.FlushLogToDataFile();

        using (var tx = Env.ReadTransaction())
        {
            var page = tx.LowLevelTransaction.GetPage(pageNum1);
            Assert.Equal(3, *page.DataPointer);
            page = tx.LowLevelTransaction.GetPage(pageNum2);
            Assert.Equal(5, *page.DataPointer);
        } 
        
        using (var tx = Env.WriteTransaction())
        {
            var page = tx.LowLevelTransaction.ModifyPage(pageNum1);
            Assert.Equal(3, *page.DataPointer);
            page = tx.LowLevelTransaction.GetPage(pageNum2);
            Assert.Equal(5, *page.DataPointer);
        }
        
        // verify again after another rollback
        using (var tx = Env.ReadTransaction())
        {
            var page = tx.LowLevelTransaction.GetPage(pageNum1);
            Assert.Equal(3, *page.DataPointer);
            page = tx.LowLevelTransaction.GetPage(pageNum2);
            Assert.Equal(5, *page.DataPointer);
        } 

    }
}
