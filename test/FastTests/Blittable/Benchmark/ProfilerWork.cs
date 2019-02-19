// -----------------------------------------------------------------------
//  <copyright file="ProfilerWork.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Linq;
using Sparrow.Json;

//using Newtonsoft.Json;

namespace FastTests.Blittable.Benchmark
{
    public class ProfilerWork
    {
        public static void Run(int take)
        {
            string directory = @"C:\Users\bumax_000\Downloads\JsonExamples";
            var files = Directory.GetFiles(directory, "*.json");
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var file in files.OrderBy(x=> new FileInfo(x).Length).Take(take))
                {
                    var v = File.ReadAllBytes(file);
                    using (blittableContext.Read(new MemoryStream(v), "doc1"))
                    {
                    }
                }
            }

        }
    }
}