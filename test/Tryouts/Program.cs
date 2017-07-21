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
                using (var test = new FastTests.Server.ServerStore())   
                {
                    try
                    {
                        test.Admin_databases_endpoint_should_fetch_document_with_etag_in_metadata_property();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.Beep();
                    }
                }
            }
        }
    }
}
