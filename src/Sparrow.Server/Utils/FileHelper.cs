using System.IO;

namespace Sparrow.Server.Utils
{
    public static class FileHelper
    {
        public static unsafe int CalculateHash(string filePath)
        {
            if (File.Exists(filePath) == false)
                return 0;

            using (var stream = File.OpenRead(filePath))
            {
                var ctx = Hashing.Streamed.XXHash32.BeginProcess();

                byte[] temp = null;
                var readBuffer = new byte[4096];

                fixed (byte* buffer = readBuffer)
                {
                    while (true)
                    {
                        var toProcess = stream.Read(readBuffer, 0, readBuffer.Length);
                        if (toProcess <= 0)
                            break;

                        var current = buffer;
                        do
                        {
                            if (toProcess < Hashing.Streamed.XXHash32.Alignment)
                            {
                                if (temp == null)
                                    temp = new byte[Hashing.Streamed.XXHash32.Alignment];

                                fixed (byte* tempBuffer = temp)
                                {
                                    Memory.Set(tempBuffer, 0, temp.Length);
                                    Memory.Copy(tempBuffer, current, toProcess);

                                    ctx = Hashing.Streamed.XXHash32.Process(ctx, tempBuffer, temp.Length);
                                    break;
                                }
                            }

                            ctx = Hashing.Streamed.XXHash32.Process(ctx, current, Hashing.Streamed.XXHash32.Alignment);
                            toProcess -= Hashing.Streamed.XXHash32.Alignment;
                            current += Hashing.Streamed.XXHash32.Alignment;
                        } while (toProcess > 0);
                    }
                }

                return (int)Hashing.Streamed.XXHash32.EndProcess(ctx);
            }
        }
    }
}
