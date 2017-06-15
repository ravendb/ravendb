using System.IO;
using System.Threading;

namespace Voron.Impl.Backup
{
    public static class StreamExtensions
    {
        private const int DefaultBufferSize = 81920;

        public static void CopyTo(this Stream source, Stream destination, CancellationToken cancellationToken)
        {
            var buffer = new byte[DefaultBufferSize];
            int count;
            while ((count = source.Read(buffer, 0, buffer.Length)) != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                destination.Write(buffer, 0, count);
            }
        }
    }
}