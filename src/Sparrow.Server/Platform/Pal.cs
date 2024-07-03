using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Sparrow.Server.Platform
{
    public static unsafe class Pal
    {
        public static PalDefinitions.SystemInformation SysInfo;
        public const int PAL_VER = 52001; // Should match auto generated rc from rvn_get_pal_ver() @ src/rvngetpalver.c

        static Pal()
        {
            PalFlags.FailCodes rc;
            int errorCode;
            try
            {
                var mutator = PlatformDetails.IsWindows8OrNewer == false ? (Func<string,string>)ToWin7DllName : default;
                DynamicNativeLibraryResolver.Register(typeof(Pal).Assembly, LIBRVNPAL, mutator);

                var palVer = rvn_get_pal_ver();
                if (palVer != 0 && palVer != PAL_VER)
                {
                    throw new IncorrectDllException(
                        $"{LIBRVNPAL} version '{palVer}' mismatches this RavenDB instance version (set to '{PAL_VER}'). Did you forget to set new value in 'rvn_get_pal_ver()'");
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

            string ToWin7DllName(string name)
            {
                return name.Replace("win", "win7");
            }
        }

        private const string LIBRVNPAL = "librvnpal";


        [Flags]
        public enum OpenFileFlags
        {
            None = 0,
            Temporary = 1 << 1,
            ReadOnly = 1 << 2,
            SequentialScan = 1 << 3,
            WritableMap = 1 << 4,
            Encrypted = 1 << 5,
            LockMemory = 1 << 6,
            DoNotConsiderMemoryLockFailureAsCatastrophicError = 1 << 7,
            CopyOnWrite = 1 << 8,
            DoNotMap = 1 << 9,
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_unmap_memory(void* mem,
            out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_map_memory(void* handle,
            Int64 offset,
            Int64 size,
            out void* mem,
            out Int32 errorCode);
            
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_close_pager(
            void* handle, void* memory, out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_sync_pager(
            void* handle, out Int32 errorCode);

        public static PalFlags.FailCodes rvn_init_pager(
            string filename,
            Int64 initialFileSize,
            OpenFileFlags flags,
            out void* handle,
            out byte* baseAddress,
            out Int64 memorySize,
            out Int32 errorCode)
        {
            using var convert = new Converter(filename);
            return rvn_init_pager(convert.Pointer, initialFileSize, flags,
                out handle, out baseAddress, out memorySize, out errorCode);
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_increase_pager_size(
            void* handle,
            Int64 newFileSize,
            out void* newHandle,
            out byte* baseAddress,
            out Int64 memorySize,
            out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_init_pager(
            byte* filename,
            Int64 initialFileSize,
            OpenFileFlags flags,
            out void* handle,
            out byte* baseAddress,
            out Int64 memorySize,
            out Int32 errorCode);

        public static PalFlags.FailCodes rvn_write_header(
            string filename,
            void* header,
            Int32 size,
            out Int32 errorCode)
        {
            using var convert = new Converter(filename);
            return rvn_write_header(convert.Pointer,
                header,
                size,
                out errorCode);
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


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_pager_get_file_handle(
            void* handle,
            out SafeFileHandle fileHandle,
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
        public static extern PalFlags.FailCodes rvn_discard_virtual_memory(
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
            private GCHandle PinnedHandle;

            public Converter(string s)
            {
                var size = Encoding.UTF8.GetMaxByteCount(s.Length) + sizeof(char);
                _buffer = ArrayPool<byte>.Shared.Rent(size);
                int length = Encoding.UTF8.GetBytes(s, 0, s.Length, _buffer, 0);
                if (length > size - sizeof(char))
                {
                    throw new InvalidOperationException(
                        $"Invalid length of GetBytes while converting string : '{s}' using '{Encoding.UTF8.EncodingName}' Encoder. Got length of {length} bytes while max size for the string using this encoder is {Encoding.UTF8.GetMaxByteCount(s.Length)}");
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
