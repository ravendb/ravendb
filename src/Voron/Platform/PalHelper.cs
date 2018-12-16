using System;
using System.IO;
using static Voron.Platform.PalFlags;
// ReSharper disable StringLiteralTypo

namespace Voron.Platform
{
    public static class PalHelper
    {
        public static void ThrowLastError(PalFlags.Errno lastError, string msg)
        {
            if (Enum.IsDefined(typeof(Errno), lastError) == false)
                throw new InvalidOperationException($"Unknown error ='{lastError}'. Message: {msg}");            
            switch (lastError)
            {
                case Errno.ENOMEM:
                    throw new OutOfMemoryException("ENOMEM on " + msg);
                case Errno.ENOENT:
                    throw new FileNotFoundException("ENOENT on " + msg);
                default:
                    throw new InvalidOperationException($"{lastError.ToString()}/{lastError} : {msg}");
            }
        }
    }
}
