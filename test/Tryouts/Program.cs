using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Tables;
using FastTests.Voron.Util;
using Raven.Server.Utils;
using SlowTests;
using SlowTests.Client.Attachments;
using SlowTests.Corax;
using SlowTests.Issues;
using SlowTests.Server.Documents.Attachments;
using SlowTests.Server.Documents.ETL;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Sharding.Cluster;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        TryRemoveDatabasesFolder();
        for (int i = 0; i < 1000; i++)
        {
            Console.WriteLine($"Starting to run {i}");


            var dt = DateTime.Now;

            dt.EnsureUtc();

            Console.WriteLine();
            try
            {
                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new S3RetiredAttachmentsSlowTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanRetireIdenticalAttachmentOnTwoDocuments_OnlyOneInCloud_AndGetFromBoth(5, new byte[] { 1, 2, 3, 4, 5 });
                //}










                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new S3RetiredAttachmentsSlowTests(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                    //
                    await test.CanUploadRetiredAttachmentToS3AndGet(1, 3);
                }




                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new AzureRetiredAttachmentsSlowTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanUploadRetiredAttachmentToAzureAndGet(1, 3);
                //}



                //CanUploadRetiredAttachmentToAzureAndGet


















                //CanInsertThenReadByDynamic

                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new RavenDB_17760(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    test.CanInsertThenReadByDynamic();
                //}

                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new S3RetiredAttachmentsSlowTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanCrudAttachmentWhenHaveRetiredAttachment( false);
                //}

                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new S3RetiredAttachmentsBackupRestoreTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanBackupAndRestoreRetiredAttachmentsWithIncrementalBackups(1, 3);
                //}


                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new DocumentSessionRetiredAttachmentsTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanDeleteRetiredAttachmentByDocumentIdAndNameAndRead(true);
                //}









                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new RavenDB_11891(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    test.Should_filter_out_deletions_using_generic_delete_behavior();
                //}



                //
                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new DocumentSessionRetiredAttachmentsTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanOverwriteRetireAttachment(new byte[] { 1,2,3 });
                //}




                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new RavenDB_11379(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.Should_remove_attachment2(RavenTestBase.Options.ForMode(RavenDatabaseMode.Single));
                //}



                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new S3RetiredAttachmentsSlowTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //       await test.CanExternalReplicateDeletedRetiredAttachmentsToDestination(1,3,false);
                //    //
                //    //await test.CanCrudAttachmentWhenHaveRetiredAttachment(true);
                //}



                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new S3RetiredAttachmentsSlowTests(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //    await test.CanExternalReplicateDeletedRetiredAttachmentsToDestination(1, 3, false);
                //}

                //

                //using (var testOutputHelper = new ConsoleTestOutputHelper())
                //using (var test = new RavenDB_22226(testOutputHelper))
                //{
                //    DebuggerAttachedTimeout.DisableLongTimespan = true;
                //    //   await test.AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig(1, 3);
                //    //
                //     test.CanInsertUpdateThenReadByDynamic();
                //}
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }

    private static void TryRemoveDatabasesFolder()
    {
        var p = System.AppDomain.CurrentDomain.BaseDirectory;
        var dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
        }
    }
}
