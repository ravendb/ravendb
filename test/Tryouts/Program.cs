using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Bond;
using Voron.Impl.FileHeaders;
using Voron.Platform.Win32;
using Voron.Tests.Backups;
using Voron.Tests.Bugs;
using Voron.Tests.RawData;
using Voron.Tests.Storage;
using Voron.Tests.Trees;
using Marshal = System.Runtime.InteropServices.Marshal;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 10; i++)
            {
                var x = new SplittingVeryBig();
                x.ShouldBeAbleToWriteValuesGreaterThanLogAndRecoverThem();
                x.Dispose();
                Console.WriteLine(i);
            }
        }
    }
}
