using System;
using System.IO;

namespace Sparrow.Server.Utils
{
    public static class FileHelper
    {
        public static unsafe int CalculateHash(string filePath)
        {
            if (File.Exists(filePath) == false)
                return 0;

            using var stream = File.OpenRead(filePath);
            var ctx = new Hashing.Streamed.XXHash32Context();
            Hashing.Streamed.XXHash32.BeginProcess(ref ctx);

            const int len = 1024*16;
            var buffer = stackalloc byte[len];
            Span<byte> span = new Span<byte>(buffer, len);

            while (true)
            {
                var toProcess = stream.Read(span);
                if (toProcess <= 0)
                    break;

                Hashing.Streamed.XXHash32.Process(ref ctx, buffer, toProcess);
            }

            return (int)Hashing.Streamed.XXHash32.EndProcess(ref ctx);
        }
    }
}
