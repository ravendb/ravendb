using System.IO;
using System.IO.Compression;

namespace Raven.Server.Utils
{
    public sealed class DeleteOnCloseZipArchive : ZipArchive
    {
        private readonly string _filePath;

        public DeleteOnCloseZipArchive(Stream stream, ZipArchiveMode mode) : base(stream, mode)
        {
            if (stream is FileStream fs)
                _filePath = fs.Name;
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                base.Dispose(disposing);
            }
            finally
            {
                if (_filePath != null)
                    PosixFile.DeleteOnClose(_filePath);
            }
        }
    }
}
