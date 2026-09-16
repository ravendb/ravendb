using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;

namespace Sparrow.Server.Platform
{
    public static class PalHelper
    {
        public static class ErrorCodes
        {
            public static class Windows
            {
                public const int ERROR_WRITE_PROTECT = 19;
                public const string ErrorMediaIsWriteProtectedHintMessage =
                    "This might indicate a hardware or OS issue. If you are running in the cloud, please consider contacting your provider since your volume's data might be inconsistent.";

                public const int ERROR_NOT_SUPPORTED = 50;
                public const int ERROR_TOO_MANY_LINKS = 1142;
            }

            public class Posix
            {
                public const int ENOTSUP_LINUX = 95;
                public const int ENOTSUP_MACOS = 45;
                public const int EMLINK = 31;
            }
        }

        public static bool IsHardLinkLimitError(int errorCode)
        {
            if (PlatformDetails.RunningOnWindows)
                return errorCode == ErrorCodes.Windows.ERROR_TOO_MANY_LINKS;

            return errorCode == ErrorCodes.Posix.EMLINK;
        }

        [DoesNotReturn]
        public static void ThrowLastError(PalFlags.FailCodes rc, int lastError, string msg)
        {
            string txt = CreateErrorMessage(rc, lastError, msg, out PalFlags.ErrnoSpecialCodes specialErrnoCodes);

            if ((specialErrnoCodes & PalFlags.ErrnoSpecialCodes.NoMem) != 0)
                throw new OutOfMemoryException(txt);

            if ((specialErrnoCodes & PalFlags.ErrnoSpecialCodes.NoEnt) != 0)
                throw new FileNotFoundException(txt);

            if ((specialErrnoCodes & PalFlags.ErrnoSpecialCodes.NoSpc) != 0)
            {
                // ENOSPC from io-ring creation is the native 'queue too small' sentinel, not a disk-full condition
                if (rc == PalFlags.FailCodes.FailCreateIoRing)
                    throw new InvalidOperationException(
                        $"Failed to create I/O ring, most likely because the requested queue size is too small. Check the 'Storage.IoRingQueueSize' configuration option. {txt}");

                throw new DiskFullException(txt);
            }

            if (PlatformDetails.RunningOnWindows)
            {
                if (lastError is ErrorCodes.Windows.ERROR_NOT_SUPPORTED)
                    throw new NotSupportedException(txt);

                if (lastError is ErrorCodes.Windows.ERROR_WRITE_PROTECT)
                    txt += $"{Environment.NewLine}{ErrorCodes.Windows.ErrorMediaIsWriteProtectedHintMessage}";
            }

            if (PlatformDetails.RunningOnPosix)
            {
                int enotsup = PlatformDetails.RunningOnMacOsx
                    ? ErrorCodes.Posix.ENOTSUP_MACOS
                    : ErrorCodes.Posix.ENOTSUP_LINUX;

                if (lastError == enotsup)
                    throw new NotSupportedException(txt);
            }

            throw new InvalidOperationException(txt);
        }

        public static string CreateErrorMessage(PalFlags.FailCodes rc, int lastError, string msg, out PalFlags.ErrnoSpecialCodes specialErrnoCodes)
        {
            try
            {
                return $"{GetNativeErrorString(lastError, msg, out specialErrnoCodes)}. FailCode={rc}.";
            }
            catch (OutOfMemoryException)
            {
                throw; // we can't assume anything is safe here, just re-throw
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"{lastError}:=(Failed to rvn_get_error_string - {ex.Message}): {msg}");
            }
        }

        public static unsafe string GetNativeErrorString(int lastError, string msg, out PalFlags.ErrnoSpecialCodes errnoSpecialCodes)
        {
            const int maxNativeErrorStr = 256;
            var buf = stackalloc byte[maxNativeErrorStr];

            var size = Pal.rvn_get_error_string(lastError, buf, maxNativeErrorStr, out var specialErrnoCodes);
            var nativeMsg = size >= 0 ? Encoding.UTF8.GetString(buf, size) : lastError.ToString();

            errnoSpecialCodes = (PalFlags.ErrnoSpecialCodes)specialErrnoCodes;
            return $"{msg}. Errno: {lastError}='{nativeMsg}' (rc={specialErrnoCodes})";
        }
    }
}
