using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Tests;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Tryout
{
    class Program
    {
        private static readonly Func<string, BitVector> binarize = x => BitVector.Of(x, true);
        private static readonly Func<long, BitVector> binarizeLong = x => BitVector.Of(true, x);
        private static readonly Func<BitVector, BitVector> binarizeIdentity = x => x;


        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";


        [Serializable]
        public class JavaRandom
        {
            public JavaRandom(ulong seed)
            {
                this.seed = (seed ^ 0x5DEECE66DUL) & ((1UL << 48) - 1);
            }

            public int NextInt(int n)
            {
                if (n <= 0) throw new ArgumentException("n must be positive");

                if ((n & -n) == n)  // i.e., n is a power of 2
                    return (int)((n * (long)Next(31)) >> 31);

                long bits, val;
                do
                {
                    bits = Next(31);
                    val = bits % (uint)n;
                }
                while (bits - val + (n - 1) < 0);

                return (int)val;
            }

            protected uint Next(int bits)
            {
                seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);

                return (uint)(seed >> (48 - bits));
            }

            private ulong seed;
        }    

        private static string GenerateRandomString(Random generator, int size)
        {
            var stringChars = new char[size];
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];
                
            return new String(stringChars);
        }        


        static unsafe void Main(string[] args)
        {
            int keySize = 3;
            int keysToInsert = 4;
            var generator = new Random(100);
            // var generator = new Random();

            //var values = new HashSet<string>();
            //var valuesAsBitVectors = new HashSet<BitVector>();
            //for (int i = 0; i < keysToInsert; i++)
            //{
            //    //long key = generator.Next() << 32 | generator.Next();
            //    string key = GenerateRandomString(generator, keySize);
            //    values.Add(key);
            //    valuesAsBitVectors.Add(BitVector.Of(key));
            //}

            //var watch = Stopwatch.StartNew();
            //TimeSpan totalTime = TimeSpan.Zero;

            //var tree = new ZFastTrieSortedSet<BitVector, string>(binarizeIdentity);

            //int count = 0;
            //foreach (var key in valuesAsBitVectors)
            //{
            //    tree.Add(key, string.Empty);
            //    if ( count > 90000 )
            //        tree.NodesTable.VerifyStructure();

            //    count++;
            //}
            //watch.Stop();

            //totalTime += watch.Elapsed;
            //Console.WriteLine(string.Format("ZFAST: Inserting {0} keys in {1} or {2} per second.", values.Count, watch.Elapsed, ((double)values.Count) / watch.ElapsedMilliseconds * 1000));

            //watch = Stopwatch.StartNew();
            //foreach (var key in valuesAsBitVectors)
            //{
            //    tree.Contains(key);
            //}
            //watch.Stop();

            //totalTime += watch.Elapsed;
            //Console.WriteLine(string.Format("ZFAST: Contains {0} keys in {1} or {2} per second.", values.Count, watch.Elapsed, ((double)values.Count) / watch.ElapsedMilliseconds * 1000));
            //Console.WriteLine(string.Format("ZFAST: Full course with {0} keys in {1} or {2} per second.", values.Count, totalTime, ((double)values.Count) / totalTime.TotalMilliseconds * 1000));

            //tree.NodesTable.VerifyStructure();

            //var list = new SortedList<string, string>();

            //watch = Stopwatch.StartNew();
            //foreach (var key in values)
            //{
            //    list.Add(key, key);
            //}
            //watch.Stop();
            //Console.WriteLine(string.Format("SortedList: Inserting {0} keys in {1} or {2} per second.", values.Count, watch.Elapsed, ((double)values.Count) / watch.ElapsedMilliseconds * 1000));


            //watch = Stopwatch.StartNew();
            //foreach (var key in values)
            //{
            //    list.ContainsKey(key);
            //}
            //watch.Stop();
            //Console.WriteLine(string.Format("SortedList: Contains {0} keys in {1} or {2} per second.", values.Count, watch.Elapsed, ((double)values.Count) / watch.ElapsedMilliseconds * 1000));

            //var dictionary = new SortedDictionary<string, string>();

            //totalTime = TimeSpan.Zero;

            //watch = Stopwatch.StartNew();
            //foreach (var key in values)
            //{
            //    dictionary.Add(key, key);
            //}
            //watch.Stop();

            //totalTime += watch.Elapsed;
            //Console.WriteLine(string.Format("SortedDictionary: Inserting {0} keys in {1} or {2} per second.", values.Count, watch.Elapsed, ((double)values.Count) / watch.ElapsedMilliseconds * 1000));


            //watch = Stopwatch.StartNew();
            //foreach (var key in values)
            //{
            //    dictionary.ContainsKey(key);
            //}
            //watch.Stop();

            //totalTime += watch.Elapsed;
            //Console.WriteLine(string.Format("SortedDictionary: Contains {0} keys in {1} or {2} per second.", values.Count, watch.Elapsed, ((double)values.Count) / watch.ElapsedMilliseconds * 1000));
            //Console.WriteLine(string.Format("SortedDictionary: Full course with {0} keys in {1} or {2} per second.", values.Count, totalTime, ((double)values.Count) / totalTime.TotalMilliseconds * 1000));


            Exception smallerEx = null;
            List<string> smallerRemoveSet = null;
            List<string> smallerInsertSet = null;
            ZFastTrieSortedSet<string, string> smallerTree = null;


            for (int i = 0; i < 100; i++)
            {
                var insertKeys = new List<string>();
                var removedKeys = new List<string>();

                var tree = new ZFastTrieSortedSet<string, string>(binarize);
                //var tree = new ZFastTrieSortedSet<long, long>(binarizeLong);

                if (i % 1000 == 0)
                    Console.WriteLine("Try: " + i);

                try
                {                    

                    var values = new HashSet<string>();
                    for (int j = 0; j < keysToInsert; j++)
                    {
                        //long key = generator.Next() << 32 | generator.Next();                        
                        string key = GenerateRandomString(generator, keySize);
                                                
                        if (values.Add(key))
                        {
                            insertKeys.Add(key);
                            tree.Add(key, key);
                        }
                            
                    }

                    foreach ( var key in values )
                    {
                        removedKeys.Add(key); // Add it before trying it.

                        var removed = tree.Remove(key);
                        ZFastTrieDebugHelpers.StructuralVerify(tree);

                        if (!removed) throw new Exception("Fail!");
                    }                    

                    
                }
                catch (Exception ex)
                {                    
                    Console.WriteLine("Failed: " + removedKeys.Count);

                    if (smallerRemoveSet == null || smallerRemoveSet.Count > removedKeys.Count)
                    {
                        smallerInsertSet = insertKeys;
                        smallerRemoveSet = removedKeys;
                        smallerTree = tree;
                        smallerEx = ex;
                    }
                }
            }

            if (smallerRemoveSet != null)
            {
                Console.WriteLine();
                Console.WriteLine("--- Insert order --- ");
                foreach (var key in smallerInsertSet)
                    Console.WriteLine(key);

                Console.WriteLine();
                Console.WriteLine("--- Removed order --- ");
                foreach (var key in smallerRemoveSet)
                    Console.WriteLine(key);

                Console.WriteLine();
                ZFastTrieDebugHelpers.DumpKeys(smallerTree);

                Console.WriteLine(smallerEx.StackTrace);
            }
        }
    }
}
