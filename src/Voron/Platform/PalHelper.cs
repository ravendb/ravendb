using System;
using System.IO;
using System.Text;
using Voron.Exceptions;
// ReSharper disable StringLiteralTypo

namespace Voron.Platform
{
    public static class PalHelper
    {
        public static unsafe void ThrowLastError(int lastError, string msg, bool forceThrowOutOfMemory = false)
        {
            string txt;
            try
            {
                const int maxNativeErrorStr = 256;
                var buf = stackalloc byte[maxNativeErrorStr];
                var size = Pal.rvn_get_error_string(lastError, buf, maxNativeErrorStr, out int specialErrnoCodes);

                var nativeMsg = size >= 0 ? Encoding.UTF8.GetString(buf, size) : lastError.ToString();

                txt = $"Errno: {lastError}='{nativeMsg}' (rc={specialErrnoCodes}) - '{msg}'";

                if ((specialErrnoCodes & (int)PalFlags.ERRNO_SPECIAL_CODES.ENOMEM) != 0 || forceThrowOutOfMemory)
                    throw new OutOfMemoryException(txt);

                if ((specialErrnoCodes & (int)PalFlags.ERRNO_SPECIAL_CODES.ENOENT) != 0)
                    throw new FileNotFoundException(txt);

                if ((specialErrnoCodes & (int)PalFlags.ERRNO_SPECIAL_CODES.ENOSPC) != 0)
                    throw new DiskFullException(txt);
            }
            catch (OutOfMemoryException)
            {
                throw; // we can't assume anything is safe here, just re-throw
            }
            catch (Exception ex)
            {
                txt = $"{lastError}:=(Failed to rvn_get_error_string - {ex.Message}): {msg}";
            }

            throw new InvalidOperationException(txt);
        }
    }
}
