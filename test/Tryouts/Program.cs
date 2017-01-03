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
        unsafe static void Main2(string[] args)
        {
            for (var i=0; i<100000;i++){
                var mem = ElectricFencedMemory.Allocate(10*1024*1024);
                ElectricFencedMemory.Free(mem);
                System.Console.WriteLine(i);
            }
        }
        unsafe static void Main(string[] args)
        {
            for(var i = 0; i< 20; i++)
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
                System.Console.WriteLine(ElectricFencedMemory.Allocs.Values.Sum(x=>x.Item1));
                foreach(var a in ElectricFencedMemory.Allocs.Values.Select(x=>x.Item2).Distinct()){
                    //if(a.Contains("GetLazyStringForFieldWithCaching("))
                    //continue;
                  //  System.Console.WriteLine(ElectricFencedMemory.Allocs.Values.Select(x=>x.Item2).Count(j=>j==a) + ":" + a);
                    //System.Console.WriteLine("---------");
                }
                
                System.Console.WriteLine(ElectricFencedMemory._contextCount);
                Console.WriteLine("***********************");
                Console.WriteLine("Finished");
            }

            System.Console.WriteLine("Summary");

            foreach (var doubleMemoryReleasesForPointer in ElectricFencedMemory.DoubleMemoryReleases.Values)
            {
             //   System.Console.WriteLine($"Count: {doubleMemoryReleasesForPointer.Count}");
             //   Console.WriteLine("Distinct Stacks");
                foreach (var stack in doubleMemoryReleasesForPointer.Distinct())
                {
                    Console.WriteLine(stack);
                }
             //   System.Console.WriteLine("---------");
            }

           // Console.WriteLine("Context Allocations");
            
            foreach (var allocation in ElectricFencedMemory.ContextAllocations.Values.Distinct())
            {   
              //  System.Console.WriteLine("Alloc count" + ElectricFencedMemory.ContextAllocations.Values.Count(j=>j==allocation));
              //  System.Console.WriteLine(allocation);
            }

            System.Console.WriteLine(Raven.Server.ServerWide.ServerStore.Instances);
        }
    }


}

