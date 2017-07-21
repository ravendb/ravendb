using System;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {   
                Console.WriteLine(i);
                using (var test = new SlowTests.Issues.RavenDB_5303())   
                {
                    try
                    {
                        test.can_load_documents_with_transformer_with_load_document_streaming();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.Beep();
                        return;
                    }
                }
            }
        }
    }
}
