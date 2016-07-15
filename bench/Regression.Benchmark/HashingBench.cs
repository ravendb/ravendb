using Microsoft.Xunit.Performance;
using Sparrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Regression
{
    public class HashingBench : BenchBase
    {
        private byte[] block = new byte[512];

        public HashingBench()
        {
            Random rnd = new Random(1001);
            rnd.NextBytes(block);
        }

        [Benchmark]
        public void Metro128_SmallString()
        {
            string value = "stringstringstringstringstringstringstring";

            ExecuteBenchmark(() => 
            {
                Metro128Hash hash;
                for (int i = 0; i < 1000; i++)
                    hash = Hashing.Metro128.Calculate(value, Encoding.UTF8);
            });
        }

        [Benchmark]
        public void XXHash64_SmallString()
        {
            string value = "stringstringstringstringstringstringstring";

            ExecuteBenchmark(() => 
            {
                ulong hash = 0;
                for (int i = 0; i < 1000; i++)
                    hash += Hashing.XXHash64.Calculate(value, Encoding.UTF8);
            });
        }

        [Benchmark]
        public void XXHash32_SmallString()
        {
            string value = "stringstringstringstringstringstringstring";

            ExecuteBenchmark(() => 
            {
                uint hash = 0;
                for (int i = 0; i < 1000; i++)
                    hash += Hashing.XXHash32.Calculate(value, Encoding.UTF8);
            });
        }

        [Benchmark]
        public void Metro128_Block()
        {
            ExecuteBenchmark(() => 
            {
                Metro128Hash hash;
                for (int i = 0; i < 1000; i++)
                    hash = Hashing.Metro128.Calculate(block);
            });
        }

        [Benchmark]
        public void XXHash64_Block()
        {
            ExecuteBenchmark(() => 
            {
                ulong hash = 0;
                for (int i = 0; i < 1000; i++)
                    hash += Hashing.XXHash64.Calculate(block);
            });
        }

        [Benchmark]
        public void XXHash32_Block()
        {
            ExecuteBenchmark(() => 
            {
                uint hash = 0;
                for (int i = 0; i < 1000; i++)
                    hash += Hashing.XXHash32.Calculate(block);
            });
        }
    }
}
