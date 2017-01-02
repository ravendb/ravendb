using System;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;

namespace Tryouts
{
    public class Program
    {
        unsafe static void Main(string[] args)
        {
            //for(var i = 0; i< 10000; i++)
            {
                Console.WriteLine("Start");
                Console.Out.Flush();
                try{
                using(var a = new FastTests.Client.Queries.FullTextSearchOnTags())
                {
                    Console.WriteLine("Created");
                    a.CanSearchUsingPhrase_MultipleSearches();
                }
                }
                catch(Exception ex){
                    Console.WriteLine(ex);
                    Console.Out.Flush();
                }
                System.Console.WriteLine("{0:#,#} kb", PosixElectricFencedMemory.usage/1024);
                foreach(var a in ElectricFencedMemory.Allocs.Values.Distinct()){
                    if(a.Contains("GetLazyStringForFieldWithCaching("))
                    continue;
                    System.Console.WriteLine(a);
                    System.Console.WriteLine("---------");
                }

                Console.WriteLine("***********************");

                foreach (var doubleMemoryReleasesForPointer in ElectricFencedMemory.DoubleMemoryReleases.Values)
                {
                    System.Console.WriteLine($"Count: {doubleMemoryReleasesForPointer.Count}");
                    Console.WriteLine("Distinct Stacks");
                    foreach (var stack in doubleMemoryReleasesForPointer.Distinct())
                    {
                        Console.WriteLine(stack);
                    }
                    System.Console.WriteLine("---------");
                }
                Console.WriteLine("Finished");
            }
        }
    }


}

