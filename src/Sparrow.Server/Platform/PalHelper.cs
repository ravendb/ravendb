using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Sparrow.Server.Exceptions;

namespace Sparrow.Server.Platform
{
    public static class PalHelper
    {
        private const int ERROR_WRITE_PROTECT = 19; 
        public const string ErrorMediaIsWriteProtectedHintMessage =
            "This might indicate a hardware or OS issue. If you are running in the cloud, please consider contacting your provider since your volume's data might be inconsistent.";

        [DoesNotReturn]
        public static void ThrowLastError(PalFlags.FailCodes rc, int lastError, string msg)
        {
            string txt;
            PalFlags.ErrnoSpecialCodes specialErrnoCodes;

            try
            {
                txt = $"{GetNativeErrorString(lastError, msg, out specialErrnoCodes)}. FailCode={rc}.";
            }
            catch (OutOfMemoryException)
            {
                throw; // we can't assume anything is safe here, just re-throw
            }
            catch (Exception ex)
            {
                txt = $"{lastError}:=(Failed to rvn_get_error_string - {ex.Message}): {msg}";
                throw new InvalidOperationException(txt);
            }

            if ((specialErrnoCodes & PalFlags.ErrnoSpecialCodes.NoMem) != 0)
                throw new OutOfMemoryException(txt);

            if ((specialErrnoCodes & PalFlags.ErrnoSpecialCodes.NoEnt) != 0)
                throw new FileNotFoundException(txt);

            if ((specialErrnoCodes & PalFlags.ErrnoSpecialCodes.NoSpc) != 0)
                throw new DiskFullException(txt);

            if (lastError is ERROR_WRITE_PROTECT)
                txt += $"{Environment.NewLine}{ErrorMediaIsWriteProtectedHintMessage}";

            throw new InvalidOperationException(txt);
        }

        public static unsafe string GetNativeErrorString(int lastError, string msg, out PalFlags.ErrnoSpecialCodes errnoSpecialCodes)
        {
            const int maxNativeErrorStr = 256;
            var buf = stackalloc byte[maxNativeErrorStr];

            var size = Pal.rvn_get_error_string(lastError, buf, maxNativeErrorStr, out var specialErrnoCodes);
            var nativeMsg = size >= 0 ? Encoding.UTF8.GetString(buf, size) : lastError.ToString();

            errnoSpecialCodes = (PalFlags.ErrnoSpecialCodes)specialErrnoCodes;
            return $"Errno: {lastError}='{nativeMsg}' (rc={specialErrnoCodes}) - '{msg}'";
        }
    }
}
