using System;
using System.Diagnostics;
using FastTests.Client.Indexing;
using FastTests.Client.Subscriptions;
using FastTests.Server.Documents.Queries;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();


            for (int i = 0; i < 128; i++)
            {
                Console.WriteLine(i);

                using (var a = new FastTests.Server.Replication.ReplicationCleanTombstones())
                {
                    a.CleanTombstones().Wait();
                }

                using (var a = new IndexesFromClient())
                {
                    a.CanStopAndStart().Wait();
                }

                using (var a = new WaitingForNonStaleResults())
                {
                    a.Throws_if_exceeds_timeout();
                }

                using (var a = new SubscriptionOperationsSignaling())
                {
                    a.SubscriptionInterruptionEventIsFiredWhenSubscriptionIsDeleted();
                }
            }

            /*using (var a = new AttachmentsSession())
            {
                a.PutAttachmentAndDeleteShouldThrow();
            }
            using (var a = new CRUD())
            {
                a.CRUD_Operations_with_what_changed();
            }
            using (var a = new AttachmentsReplication())
            {
                a.PutSameAttachmentsDifferentContentTypeShouldConflict().Wait();
            }
            using (var a = new ReplicationOfConflicts())
            {
                a.ReplicateTombstoneConflict().Wait();
            }
            using (var a = new AttachmentsSession())
            {
                a.PutAttachments();
            }
            using (var a = new FirstClassPatch())
            {
                a.CanPatchAndModify();
            }
            using (var a = new Advanced())
            {
                a.CanUseDefer();
            }*/
        }
    }
}