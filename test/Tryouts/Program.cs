using System;
using FastTests.Server.Documents.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, "E:\\Work");
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var store = new FastTests.NewClient.CRUD())
                {
                    store.CRUD_Operations_With_Mark_Read_Only();
                }
            }
        }
    }

}

