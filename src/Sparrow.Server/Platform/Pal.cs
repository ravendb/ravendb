using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Sparrow.Platform;
using Sparrow.Server.Utils;

namespace Sparrow.Server.Platform
{
    public static unsafe class Pal
    {
        public static PalDefinitions.SystemInformation SysInfo;
        public const int PAL_VER = 42012; // Should match auto generated rc from rvn_get_pal_ver() @ src/rvngetpalver.c

        static Pal()
        {
            var toFilename = LIBRVNPAL;
            string fromFilename;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                if (RuntimeInformation.ProcessArchitecture != Architecture.Arm &&
                    RuntimeInformation.ProcessArchitecture != Architecture.Arm64)
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.linux.x64.so" : $"{toFilename}.linux.x86.so";
                    toFilename += ".so";
                }
                else
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.arm.64.so" : $"{toFilename}.arm.32.so";
                    toFilename += ".so";
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.mac.x64.dylib" : $"{toFilename}.mac.x86.dylib";
                // in mac we are not : `toFilename += ".so";` as DllImport doesn't assume .so nor .dylib by default
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var win7 = PlatformDetails.IsWindows8OrNewer ? "" : "7";

                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.win{win7}.x64.dll" : $"{toFilename}.win{win7}.x86.dll";
                toFilename += ".dll";
            }
            else
            {
                throw new NotSupportedException("Not supported platform - no Windows/OSX/Linux is detected ");
            }

            var retries = 0;
            while (true)
            {
                var e = CopyPalFile(toFilename, fromFilename);
                if (e == null)
                    break;

                if (++retries < 10)
                    continue;

                var msg = $"Cannot copy {fromFilename} to {toFilename}";
                if (e is IOException)
                {
                    throw new IOException($"{msg}, make sure appropriate {toFilename} to your platform architecture exists in Raven.Server executable folder.", e);
                }

                throw new InvalidOperationException($"{msg}.", e);
            }

            PalFlags.FailCodes rc = PalFlags.FailCodes.None;
            int errorCode;
            try
            {
                var palver = rvn_get_pal_ver();
                if (palver != 0 && palver != PAL_VER)
                {
                    throw new IncorrectDllException(
                        $"{LIBRVNPAL} version '{palver}' mismatches this RavenDB instance version (set to '{PAL_VER}'). Either use correct {fromFilename}, or a new one returning zero in 'rvn_get_pal_ver()'");
                }

                rc = rvn_get_system_information(out SysInfo, out errorCode);
            }
            catch (Exception ex)
            {
                var errString = $"{LIBRVNPAL} version might be invalid, missing or not usable on current platform.";

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    errString +=
                        " Initialization error could also be caused by missing 'Microsoft Visual C++ 2015 Redistributable Package' (or newer). It can be downloaded from https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads.";

                errString += $" Arch: {RuntimeInformation.OSArchitecture}, OSDesc: {RuntimeInformation.OSDescription}";

                throw new IncorrectDllException(errString, ex);
            }

            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, "Cannot get system information");
        }

        private static Exception CopyPalFile(string toFilename, string fromFilename)
        {
            try
            {
                var copy = true;
                if (File.Exists(toFilename))
                {
                    var fromHash = FileHelper.CalculateHash(fromFilename);
                    var toHash = FileHelper.CalculateHash(toFilename);

                    copy = fromHash != toHash;
                }

                if (copy)
                    File.Copy(fromFilename, toFilename, overwrite: true);

                return null;
            }
            catch (Exception e)
            {
                Thread.Sleep(100);
                return e;
            }
        }

        private const string LIBRVNPAL = "librvnpal";

        public static PalFlags.FailCodes rvn_write_header(
            string filename,
            void* header,
            Int32 size,
            out Int32 errorCode)
        {
            using (var convert = new Converter(filename))
            {
                return rvn_write_header(convert.Pointer,
                    header,
                    size,
                    out errorCode);
            }
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_write_header(
            byte* filename,
            void* header,
            Int32 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_get_error_string(
            Int32 errorCode,
            void* sb,
            Int32 capacity,
            out Int32 specialErrnoCodes);

        public static PalFlags.FailCodes rvn_create_and_mmap64_file(
            string filename,
            Int64 initialFileSize,
            PalFlags.MmapOptions flags,
            out SafeMmapHandle handle,
            out void* baseAddress,
            out Int64 actualFileSize,
            out Int32 errorCode)
        {
            using (var convert = new Converter(filename))
            {
                return rvn_create_and_mmap64_file(convert.Pointer,
                    initialFileSize,
                    flags,
                    out handle,
                    out baseAddress,
                    out actualFileSize,
                    out errorCode);
            }
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_create_and_mmap64_file(
            byte* filename,
            Int64 initialFileSize,
            PalFlags.MmapOptions flags,
            out SafeMmapHandle handle,
            out void* baseAddress,
            out Int64 actualFileSize,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_prefetch_virtual_memory(
            void* virtualAddress,
            Int64 length,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern PalFlags.FailCodes rvn_get_system_information(
            out PalDefinitions.SystemInformation systemInformation,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_memory_sync(
            void* address,
            Int64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_mmap_dispose_handle(
            IntPtr handle,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_unmap(
            PalFlags.MmapOptions flags,
            void* address,
            Int64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_prefetch_ranges(
            PalDefinitions.PrefetchRanges* list,
            Int32 count,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_protect_range(
            void* start,
            Int64 size,
            PalFlags.ProtectRange protection,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_allocate_more_space(
            Int64 newLengthAfterAdjustment,
            SafeMmapHandle handle,
            out void* newAddress,
            out Int32 errorCode);

        public static PalFlags.FailCodes rvn_open_journal_for_writes(
            string filename,
            PalFlags.JournalMode mode,
            Int64 requiredSize,
            PalFlags.DurabilityMode supportDurability,
            out SafeJournalHandle handle,
            out Int64 actualSize,
            out Int32 errorCode)
        {
            using (var convert = new Converter(filename))
            {
                return rvn_open_journal_for_writes(convert.Pointer,
                    mode,
                    requiredSize,
                    supportDurability,
                    out handle,
                    out actualSize,
                    out errorCode);
            }
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_open_journal_for_writes(
            byte* fileName,
            PalFlags.JournalMode mode,
            Int64 requiredSize,
            PalFlags.DurabilityMode supportDurability,
            out SafeJournalHandle handle,
            out Int64 actualSize,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_close_journal(
            IntPtr handle,
            out Int32 errorCode
        );


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_write_journal(
            SafeJournalHandle handle,
            void* buffer,
            Int64 size,
            Int64 offset,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_read_journal(
            SafeJournalHandle handle,
            void* buffer,
            Int64 requiredSize,
            Int64 offset,
            out Int64 actualSize,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_truncate_journal(
            SafeJournalHandle handle,
            Int64 size,
            out Int32 errorCode
        );

        public static PalFlags.FailCodes rvn_get_path_disk_space(
            string filename,
            out UInt64 totalFreeSizeInBytes,
            out UInt64 totalSizeInBytes,
            out Int32 errorCode)
        {
            using (var convert = new Converter(filename))
            {
                return rvn_get_path_disk_space(convert.Pointer,
                    out totalFreeSizeInBytes,
                    out totalSizeInBytes,
                    out errorCode);
            }
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_get_path_disk_space(
            byte* path,
            out UInt64 totalFreeSizeInBytes,
            out UInt64 totalSizeInBytes,
            out Int32 errorCode
        );

        public static PalFlags.FailCodes rvn_open_journal_for_reads(
            string filename,
            out SafeJournalHandle handle,
            out Int32 errorCode)
        {
            using (var convert = new Converter(filename))
            {
                return rvn_open_journal_for_reads(convert.Pointer,
                    out handle,
                    out errorCode);
            }
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_open_journal_for_reads(
            byte* fileNameFullPath,
            out SafeJournalHandle handle,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_discard_virtual_memory(
            void* address,
            Int64 size,
            out Int32 errorCode);

        public static PalFlags.FailCodes rvn_test_storage_durability(
            string tempFilename,
            out Int32 errorCode)
        {
            using (var convert = new Converter(tempFilename))
            {
                return rvn_test_storage_durability(convert.Pointer,
                    out errorCode);
            }
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_test_storage_durability(
            byte* tempFilename,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_get_pal_ver();

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int64 rvn_get_current_thread_id();

        private struct Converter : IDisposable
        {
            private byte[] _buffer;
            public byte* Pointer => (byte*)PinnedHandle.AddrOfPinnedObject();
            private static readonly Encoding CurrentEncoding = PlatformDetails.RunningOnPosix ? Encoding.UTF8 : Encoding.Unicode;
            private GCHandle PinnedHandle;

            public Converter(string s)
            {
                var size = CurrentEncoding.GetMaxByteCount(s.Length) + sizeof(char);
                _buffer = ArrayPool<byte>.Shared.Rent(size);
                int length = CurrentEncoding.GetBytes(s, 0, s.Length, _buffer, 0);
                if (length > size - sizeof(char))
                {
                    throw new InvalidOperationException(
                        $"Invalid length of GetBytes while converting string : '{s}' using '{CurrentEncoding.EncodingName}' Encoder. Got length of {length} bytes while max size for the string using this encoder is {CurrentEncoding.GetMaxByteCount(s.Length)}");
                }

                for (int i = length; i < length + sizeof(char); i++)
                {
                    _buffer[i] = 0;
                }

                
                PinnedHandle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            }

            public void Dispose()
            {
                PinnedHandle.Free();
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = null;
            }
        }
    }
}
