using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler.Data
{
    public class FileUploadUnitOfWork
    {
        public ZipArchiveEntry ZipEntry { get; private set; }
        public FileHeader Header { get; private set; }

        public FileUploadUnitOfWork(ZipArchiveEntry zipEntry, FileHeader header)
        {
            this.ZipEntry = zipEntry;
            this.Header = header;
        }
    }

    public class FilesUploadWorker
    {
        private readonly FileUploadUnitOfWork[] filesAndMetadata;

        public FilesUploadWorker(FileUploadUnitOfWork[] filesAndMetadata)
        {
            if (filesAndMetadata == null || filesAndMetadata.Length == 0)
                throw new ArgumentException("filesAndMetadata cannot be empty");

            this.filesAndMetadata = filesAndMetadata;
        }

        public async Task UploadFiles(Stream netStream, TaskCompletionSource<object> t)
        {
            /* format for each entry: 
             * name (binary writer string)
             * metadata (binary writer string)
             * fileSize (long)
             * file (byte array)
             * */

            //send everything as is


            var binaryWriter = new BinaryWriter(netStream);

            var buffer = new byte[1024*8];
            foreach (var fileUploadUnitOfWork in filesAndMetadata)
            {
                var headerFullPath = fileUploadUnitOfWork.Header.FullPath;
                
                binaryWriter.Write(headerFullPath);
                binaryWriter.Write(fileUploadUnitOfWork.Header.Metadata.ToString(Formatting.None));
                binaryWriter.Write(fileUploadUnitOfWork.ZipEntry.Length);

                using (var unzippedStream = fileUploadUnitOfWork.ZipEntry.Open())
                {
                    await CopyStreamsUsingBuffer(netStream, fileUploadUnitOfWork.ZipEntry.Length, unzippedStream, buffer).ConfigureAwait(false);
                }
            }


            netStream.Flush();
            t.TrySetResult(null);
        }

        private static async Task CopyStreamsUsingBuffer(Stream netStream, long fileSize, Stream unzippedStream, byte[] fileCopyBuffer)
        {
            long bytesCopied = 0;

            while (bytesCopied < fileSize)
            {
                var bytesReadInCurrentIteration = await unzippedStream.ReadAsync(fileCopyBuffer, 0, (int)Math.Min(fileSize - bytesCopied, fileCopyBuffer.Length)).ConfigureAwait(false);
                await netStream.WriteAsync(fileCopyBuffer, 0, bytesReadInCurrentIteration).ConfigureAwait(false);
                bytesCopied += bytesReadInCurrentIteration;
            }
        }
    }
}
