using System;
using System.IO;
using System.Text;
using Voron.Exceptions;
using static Voron.Platform.PalFlags;

namespace Voron.Platform
{
    public static class PalHelper
    {
        public static void ThrowLastError(int lastError, string msg)
        {
            string txt;
            try
            {
                txt = GetNativeErrorString(lastError, msg, out var specialErrnoCodes);

                if ((specialErrnoCodes & ErrnoSpecialCodes.NoMem) != 0)
                    throw new OutOfMemoryException(txt);

                if ((specialErrnoCodes & ErrnoSpecialCodes.NoEnt) != 0)
                    throw new FileNotFoundException(txt);

                if ((specialErrnoCodes & ErrnoSpecialCodes.NoSpc) != 0)
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

        public static unsafe string GetNativeErrorString(int lastError, string msg, out ErrnoSpecialCodes errnoSpecialCodes)
        {
            const int maxNativeErrorStr = 256;
            var buf = stackalloc byte[maxNativeErrorStr];

            var size = Pal.rvn_get_error_string(lastError, buf, maxNativeErrorStr, out var specialErrnoCodes);
            var nativeMsg = size >= 0 ? Encoding.UTF8.GetString(buf, size) : lastError.ToString();

            errnoSpecialCodes = (ErrnoSpecialCodes)specialErrnoCodes;
            return $"Errno: {lastError}='{nativeMsg}' (rc={specialErrnoCodes}) - '{msg}'";
        }
    }
}
