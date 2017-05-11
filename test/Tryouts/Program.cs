using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Client;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using SlowTests.Client.Attachments;
using SlowTests.Core.Session;

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
                using (var a = new AttachmentsBigFiles())
                {
                    // a.SupportHugeAttachment_MaxLong(int.MaxValue);
                }
            }
            if (DateTime.Now > DateTime.MaxValue)
            {
                return;
            }

            using (var a = new AttachmentsSession())
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
            }
        }
    }
}