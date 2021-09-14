using System.IO;
using System.IO.Compression;

namespace Raven.Server.Utils
{
    public class DeleteOnCloseZipArchive : ZipArchive
    {
        private readonly FileStream _stream;

        public DeleteOnCloseZipArchive(Stream stream) : base(stream)
        {
            _stream = stream as FileStream;
        }

        public DeleteOnCloseZipArchive(Stream stream, ZipArchiveMode mode) : base(stream, mode)
        {
            _stream = stream as FileStream;
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                base.Dispose(disposing);
            }
            finally
            {
                if (_stream != null)
                    PosixFile.DeleteOnClose(_stream.Name);
            }
        }
    }
}
