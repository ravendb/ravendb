using System;
using SlowTests.Client.Attachments;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new AttachmentsReplication())
                {
                    test.PutSameAttachmentsDifferentContentTypeShouldConflict().Wait();
                    // test.PutDifferentAttachmentsShouldConflict().Wait();
                    // test.PutAndDeleteAttachmentsWithTheSameStream_AlsoTestBigStreams().Wait();
                }
            }
        }
    }
}
