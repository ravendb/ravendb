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


        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        private static string GenerateRandomString(Random generator, int size)
        {
            var stringChars = new char[size];
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];

            return new String(stringChars);
        }


        static unsafe void Main(string[] args)
        {
            var generator = new Random(100);

            int count = 60000;
            int size = 4;
            for (int i = 0; i < 100; i++)
            {
                var keys = new string[count];

                var tree = new ZFastTrieSortedSet<string, string>(binarize);

                try
                {
                    for (int j = 0; j < count; j++)
                    {
                        string key = GenerateRandomString(generator, size);

                        if (!tree.Contains(key))
                            tree.Add(key, key);

                        keys[j] = key;
                    }

                    ZFastTrieDebugHelpers.StructuralVerify(tree);
                }
                catch (Exception ex)
                {
                    Console.WriteLine();
                    Console.WriteLine("--- Insert order for error --- ");
                    foreach (var key in keys)
                        Console.WriteLine(key);

                    Console.WriteLine();
                    ZFastTrieDebugHelpers.DumpKeys(tree);

                    Console.WriteLine(ex.StackTrace);
                }
            }

            //var tree = new ZFastTrieSortedSet<string, string>(binarize);

            ////tree.Add("8Jp3", "8Jp3");
            ////tree.Add("GX37", "GX37");
            ////tree.Add("f04o", "f04o");
            ////tree.Add("KmGx", "KmGx");


            //tree.Add("6b", "8Jp3");
            //tree.Add("ab", "V6sl");
            //tree.Add("dG", "GX37");
            //tree.Add("3s", "f04o");
            //tree.Add("8u", "KmGx");
            //tree.Add("cI", "KmGx");

            //ZFastTrieDebugHelpers.StructuralVerify(tree);
            //ZFastTrieDebugHelpers.DumpKeys(tree);
        }
    }
}
