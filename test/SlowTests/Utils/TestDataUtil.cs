using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace SlowTests.Utils
{
    public class TestDataUtil
    {

        public static IDictionary<long, byte[]> ReadData(string fileName)
        { 
            var assembly = typeof(TestDataUtil).Assembly;
            using (var fs = assembly.GetManifestResourceStream("SlowTests.Data." + fileName))
            using (var reader = new StreamReader(fs))
            {
                string line;

                var random = new Random();
                var results = new Dictionary<long, byte[]>();

                while (!string.IsNullOrEmpty(line = reader.ReadLine()))
                {
                    var l = line.Trim().Split(':');

                    var buffer = new byte[int.Parse(l[1])];
                    random.NextBytes(buffer);

                    results.Add(long.Parse(l[0]), buffer);
                }

                return results;
            }
        }
    }
}