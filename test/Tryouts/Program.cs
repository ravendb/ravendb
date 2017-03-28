using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Smuggler;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
using Sparrow;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            using (var a = new AttachmentsCrud())
            {
                a.PutAttachments();
            }

            using (var a = new AttachmentsReplication())
            {
                a.PutDifferentAttachmentsShouldConflict().Wait();
            }
        }
    }
}