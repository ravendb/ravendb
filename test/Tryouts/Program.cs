using System.Threading.Tasks;
using SlowTests.Client.Attachments;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            using (var test = new AttachmentsSession())
                await test.PutAttachments();
        }
    }
}
