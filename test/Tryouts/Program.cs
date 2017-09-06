using System;
using System.Threading.Tasks;
using SlowTests.Server.Documents.PeriodicBackup;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Parallel.For(0, 1000, i =>
            {
                Console.WriteLine(i);
                using (var test = new SlowTests.Client.Attachments.AttachmentFailover())
                {
                    test.PutAttachmentsWithFailover_Session().Wait();
                }
            });
        }
    }
}
