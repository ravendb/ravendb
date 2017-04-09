using System;
using FastTests.Issues;
using FastTests.Server.Replication;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                using (var a = new ReplicationResolveToDatabase())
                using (var b = new ReplicationCleanTombstones())
                {
                    try
                    {
                        b.DontCleanTombstones();
                    }
                    catch
                    {
                        //
                    }
                    a.ResovleToDatabase();                                      
                }
            }
        }
    }
}