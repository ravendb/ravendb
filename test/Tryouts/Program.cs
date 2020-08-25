using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Blittable;
using SlowTests.Issues;
using SlowTests.Server.Documents.ETL.Raven;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 10_000; i++)
            {
                 Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new RavenDB_11379(testOutputHelper))
                    {
                        test.Should_remove_attachment(@"

var doc = loadToUsers(this);

var attachments = this['@metadata']['@attachments'];

for (var i = 0; i < attachments.length; i++) {
    if (attachments[i].Name.endsWith('.png'))
        doc.addAttachment(loadAttachment(attachments[i].Name));
}
", false, "photo.png", "photo.png");
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
