using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Raven.Server.Utils;
using SlowTests.Corax;
using SlowTests.Sharding.Cluster;
using Xunit;
using FastTests.Voron.Util;
using FastTests.Sparrow;
using FastTests.Voron.FixedSize;
using FastTests.Client.Indexing;
using FastTests;
using Sparrow.Server.Platform;
using SlowTests.Authentication;
using SlowTests.Issues;
using SlowTests.Server.Documents.PeriodicBackup;
using Microsoft.Diagnostics.Tracing.Parsers.MicrosoftAntimalwareEngine;
using NLog;
using RachisTests;
using SlowTests.SlowTests.MailingList;
using Voron.Impl.FreeSpace;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
     var random = new Random();
        var sbList = new List<StreamBitArray>();
        var sbList2 = new List<StreamBitArray2>();

        var whichTest = 2;
        for (var i = 0; i < 5000; i += 1)
        {
            var sba = new StreamBitArray();
            var sb2 = new StreamBitArray2();

            switch (whichTest)
            {
                case 0:
                    for (int j = 0; j < 2048; j++)
                    {
                        if (j % 32 == 0)
                        {
                            sba.Set(j, true);
                            sb2.Set(j, true);
                        }
                        else
                        {
                            sba.Set(j, false);
                            sb2.Set(j, false);
                        }
                    }

                    break;

                case 1:
                    int c = 0;
                    while (c < 2048)
                    {
                        int blockSize = random.Next(2, 8 + 1);

                        bool value = random.Next(2) == 1;

                        for (int k = 0; k < blockSize && c < 2048; k++, c++)
                        {
                            sba.Set(c, value);
                            sb2.Set(c, value);
                        }
                    }
                    break;

                case 2:
                    int consecutiveOnes = 0; // Track consecutive 1s

                    for (int j = 0; j < 2048; j += 1)
                    {
                        bool bit;

                        // Randomly decide the bit value with conditions for consecutive 1s
                        if (consecutiveOnes < 1024 && random.Next(2) == 1)
                        {
                            bit = true;
                            consecutiveOnes++;
                        }
                        else
                        {
                            bit = false;
                            consecutiveOnes = 0; // Reset count for consecutive 1s
                        }

                        //int bitPositionInSection = j % 32;
                        //bit = bitPositionInSection < 10 ? false : true;

                        // Set the same bit in both StreamBitArray and StreamBitArray2
                        sba.Set(j, bit);
                        sb2.Set(j, bit);
                    }
                    break;
            }

            sbList.Add(sba);
            sbList2.Add(sb2);
        }

        var list = new List<int>();
        for (var i = 0; i < 1024; i++)
        {
            //list.Add(1);
            list.Add(random.Next(2, 32));
            //list.Add(random.Next(32, 2049));

            //list.Add(2);
            //list.Add(random.Next(1, 2049));

            //list.Add(random.Next(1600, 2048));
            //list.Add(15);
        }

        Console.WriteLine("Verification");

        // for (var i = 0; i < sbList.Count; i++)
        // {
        //     var sb = sbList[i];
        //     var sb2 = sbList2[i];
        //     foreach (int num in list)
        //     {
        //         var num1 = sb.get(num);
        //         var num2 = sb.GetContinuousRangeStart(num);
        //         var num3 = sb2.TryGetContinuousRange(num);
        //
        //         if (num1 == num2 && num2 == num3)
        //         {
        //             // All numbers are equal
        //             //Console.WriteLine("All numbers are equal.");
        //         }
        //         else
        //         {
        //             // Not all numbers are equal
        //             Console.WriteLine($"Numbers are not equal for num = {num}, {num1}, {num2}, {num3}");
        //         }
        //     }
        // }

        Console.WriteLine("Done verification");

        var sp = Stopwatch.StartNew();
        //
        // for (var i = 0; i < sbList.Count; i++)
        // {
        //     var sb = sbList2[i];
        //     foreach (int num in list)
        //     {
        //         sb.GetContinuousRangeStartLegacy(num);
        //     }
        // }
        //
        // Console.WriteLine($"legacy: {sp.ElapsedMilliseconds}ms");

        sp.Restart();

        for (var i = 0; i < sbList.Count; i++)
        {
            var sb = sbList2[i];
            foreach (int num in list)
            {
                sb.GetContinuousRangeStart(num);
            }
        }

        Console.WriteLine($"new: {sp.ElapsedMilliseconds}ms");

        sp.Restart();

        for (var i = 0; i < sbList.Count; i++)
        {
            var sb2 = sbList[i];
            foreach (int num in list)
            {
                sb2.FindRange(num);
            }
        }
        
        Console.WriteLine($"7.0 took: {sp.ElapsedMilliseconds}ms");
    }
}
