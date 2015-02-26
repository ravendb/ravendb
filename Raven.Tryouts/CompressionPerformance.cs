using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Tar;
//using LZ4;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Tryouts
{
    public class CompressionPerformance
    {
        private const int BatchSize = 24;
        public static void Main()
        {
//            PrepareLZ4();
//            PrepareGZip();
//
//            Console.WriteLine("Loading dataset...");
//            var input = ParseDisks();
//            input.Position = 0;
//            
//            float lengthInMb = ((float)input.Length) / (1024 * 1024);
//
//            Console.WriteLine("Stream size: " + lengthInMb + "Mb");
//            Console.WriteLine();
//
//            var lzInput = new MemoryStream();
//            input.CopyTo(lzInput);
//            input.Position = 0;
//            lzInput.Position = 0;
//            BenchmarkLZ4(lzInput, lengthInMb);
//           
//            var gzipInput = new MemoryStream();
//            input.CopyTo(gzipInput);
//            input.Position = 0;
//            gzipInput.Position = 0;
//            BenchmarkGZip(gzipInput, lengthInMb);
//
//            Console.ReadLine();
        }

//        private static void PrepareLZ4()
//        {
//            byte[] data = new byte[1024];
//            Random rnd = new Random(100);
//            rnd.NextBytes(data);
//
//            var input = new MemoryStream(data);
//
//            var compressed = new MemoryStream();
//            var compressLz4Stream = new LZ4Stream(compressed, CompressionMode.Compress);
//            input.CopyTo(compressLz4Stream);
//            compressLz4Stream.Flush();
//
//            compressed.Position = 0;
//
//            var decompressed = new MemoryStream();
//            var decompressLz4Stream = new LZ4Stream(compressed, CompressionMode.Decompress);
//            decompressLz4Stream.CopyTo(decompressed);
//            decompressLz4Stream.Flush();
//
//            decompressed.Position = 0;
//
//            for (int i = 0; i < data.Length; i++)
//            {
//                int b = decompressed.ReadByte();
//                if (data[i] != b)
//                    Debugger.Break();
//            }            
//        }
//
//        private static void PrepareGZip()
//        {
//            byte[] data = new byte[1024];
//            Random rnd = new Random(100);
//            rnd.NextBytes(data);
//            
//            var input = new MemoryStream(data);
//
//            var compressed = new MemoryStream();
//            using (var compressGzipStream = new GZipStream(compressed, CompressionMode.Compress, true))
//            {
//                input.CopyTo(compressGzipStream);
//            }
//
//            compressed.Position = 0;
//
//            var decompressed = new MemoryStream();
//            using (var decompressGzipStream = new GZipStream(compressed, CompressionMode.Decompress, true))
//            {
//                decompressGzipStream.CopyTo(decompressed);
//            }
//
//            decompressed.Position = 0;
//
//            for (int i = 0; i < data.Length; i++)
//            {
//                int b = decompressed.ReadByte();
//                if (data[i] != b)
//                    Debugger.Break();
//            }                    
//        }
//
//        private static void BenchmarkLZ4(MemoryStream input, float lengthInMb)
//        {
//            Console.WriteLine("LZ4");
//
//            Stopwatch sp = new Stopwatch();
//            sp.Start();
//
//            var compressed = new MemoryStream();
//            var compressLz4Stream = new LZ4Stream(compressed, CompressionMode.Compress);
//            input.CopyTo(compressLz4Stream);
//            compressLz4Stream.Flush();
//
//            var compressElapsed = sp.Elapsed;
//
//            Console.WriteLine(string.Format("Compressed Size: {0}", ((float)compressed.Length) / 1024 * 1024));
//
//            compressed.Position = 0;
//
//            sp.Restart();
//            var decompressed = new MemoryStream();
//            var decompressLz4Stream = new LZ4Stream(compressed, CompressionMode.Decompress);
//            decompressLz4Stream.CopyTo(decompressed);
//            decompressLz4Stream.Flush();
//
//            var decompressElapsed = sp.Elapsed;
//
//            Console.WriteLine(string.Format("Compress: {0} Mb/sec. - {1}", lengthInMb / compressElapsed.TotalSeconds, compressElapsed));
//            Console.WriteLine(string.Format("Decompress: {0} Mb/sec. - {1}", lengthInMb / decompressElapsed.TotalSeconds, decompressElapsed));
//            Console.WriteLine();
//        }
//
//        private static void BenchmarkGZip(MemoryStream input, float lengthInMb)
//        {
//            Console.WriteLine("GZip");
//
//            Stopwatch sp = new Stopwatch();
//            sp.Start();
//
//            var compressed = new MemoryStream();
//            using (var compressGzipStream = new GZipStream(compressed, CompressionMode.Compress, true))
//            {
//                input.CopyTo(compressGzipStream);
//            }
//            var compressElapsed = sp.Elapsed;
//
//            Console.WriteLine(string.Format("Compressed Size: {0}", ((float)compressed.Length) / 1024 * 1024));
//
//            compressed.Position = 0;
//
//            sp.Restart();
//            var decompressed = new MemoryStream();
//            using (var decompressGzipStream = new GZipStream(compressed, CompressionMode.Decompress, true))
//            {
//                decompressGzipStream.CopyTo(decompressed);
//            }
//            var decompressElapsed = sp.Elapsed;
//
//            Console.WriteLine(string.Format("Compress: {0} Mb/sec. - {1}", lengthInMb / compressElapsed.TotalSeconds, compressElapsed));
//            Console.WriteLine(string.Format("Decompress: {0} Mb/sec. - {1}", lengthInMb / decompressElapsed.TotalSeconds, decompressElapsed));
//            Console.WriteLine();
//        }
//
//        private static MemoryStream ParseDisks()
//        {
//            var output = new MemoryStream();
//
//            int i = 0;
//            var parser = new Parser();
//            var buffer = new byte[1024 * 1024];// more than big enough for all files
//
//            using (var bz2 = new BZip2InputStream(File.Open(@"D:\Scratch\freedb-complete-20150101.tar.bz2", FileMode.Open)))
//            using (var tar = new TarInputStream(bz2))
//            using (var fileWriter = new StreamWriter(output, Encoding.UTF8, 4096, leaveOpen: true))
//            {
//                int processed = 0;
//
//                TarEntry entry;
//                while ((entry = tar.GetNextEntry()) != null)
//                {
//                    if (processed > 10000)
//                        return output;
//
//                    if (entry.Size == 0 || entry.Name == "README" || entry.Name == "COPYING")
//                        continue;
//
//                    var readSoFar = 0;
//                    while (true)
//                    {
//                        var read = tar.Read(buffer, readSoFar, ((int)entry.Size) - readSoFar);
//                        if (read == 0)
//                            break;
//
//                        readSoFar += read;
//                    }
//
//                    // we do it in this fashion to have the stream reader detect the BOM / unicode / other stuff
//                    // so we can read the values properly
//                    var fileText = new StreamReader(new MemoryStream(buffer, 0, readSoFar)).ReadToEnd();
//                    fileWriter.Write(fileText);
//                    processed++;
//                }
//            }
//
//            return output;
//        }
    }	
}
