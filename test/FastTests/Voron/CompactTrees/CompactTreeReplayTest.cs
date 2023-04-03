using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Voron;
using Xunit.Abstractions;

namespace FastTests.Voron.CompactTrees
{
    public class CompactTreeReplayTest : StorageTest
    {
        public CompactTreeReplayTest(StorageEnvironmentOptions options, ITestOutputHelper output) : base(options, output)
        { }

        public CompactTreeReplayTest(ITestOutputHelper output) : base(output)
        { }

        protected static IEnumerable<List<string>> ReadTermsFromResource(string file)
        {
            Stream inputFile = typeof(RavenDB_19703).Assembly.GetManifestResourceStream("FastTests.Voron.CompactTrees." + file);
            if (Path.GetExtension(file) == "gz")
                inputFile = new GZipStream(inputFile, CompressionMode.Decompress);

            var reader = new StreamReader(inputFile);
            var adds = new List<string>();
            while (reader.ReadLine() is { } line)
            {
                if (line.StartsWith("#"))
                {
                    yield return adds;
                    adds.Clear();
                    continue;
                }

                adds.Add(line);
            }

            yield return adds;
        }
    }
}
