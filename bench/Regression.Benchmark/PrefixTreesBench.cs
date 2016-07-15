using Microsoft.Xunit.Performance;
using Sparrow.Binary;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Regression
{
    public class PrefixTreesBench : BenchBase
    {
        private readonly Func<string, BitVector> binarize = x => BitVector.Of(true, Encoding.UTF8.GetBytes(x));

        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        private static string GenerateRandomString(Random generator, int size)
        {
            var stringChars = new char[size];
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];

            return new String(stringChars);
        }

        string[] values;

        public PrefixTreesBench()
        {
            Random rnd = new Random(1000);

            values = new string[1000];
            for (int i = 0; i < values.Length; i++)
                values[i] = GenerateRandomString(rnd, 15);
        }

        [Benchmark]
        public void AdditionAndRemoval()
        {
            ExecuteBenchmark(() => 
            {
                var tree = new ZFastTrieSortedSet<string, string>(binarize);
                for (int i = 0; i < 1000; i++)
                    tree.Add(values[i], string.Empty);

                for (int i = 0; i < 1000; i++)
                    tree.Remove(values[i]);
            });            
        }        
    }
}
