using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new ParserTests())
                {
                    test.ParseAndWriteAst(q: "(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", o:
                        "{\"Type\":\"Or\",\"Left\":{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Left\":\"State\",\"Right\":2},\"Right\":{\"Type\":\"Equal\",\"Left\":\"Act\",\"Right\":\"Wait\"}},\"Right\":{\"Type\":\"Not\",\"Expression\":{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Admin\"}}}");
                }
            }
        }
    }
}
