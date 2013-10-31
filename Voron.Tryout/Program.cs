using System;
using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;
using Voron.Tests.Storage;

namespace Voron.Tryout
{
    internal class Program
    {
        private static unsafe void Main()
        {
	        for (int i = 0; i < 10000; i++)
	        {
		        Console.WriteLine(i);
		        using (var x = new Batches())
		        {
			        x.ReadVersion_Items_From_Both_WriteBatch_And_Snapshot_Deleted_Key_Returns_Version0();
		        }
	        }
        }
    }
}