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
        public const int PAL_VER = 70841; // Should match auto generated rc from rvn_get_pal_ver() @ src/rvngetpalver.c

        static Pal()
        {
            PalFlags.FailCodes rc;
            int errorCode;
            PalDefinitions.SystemInformation sysInfo; 
            try
            {
                var cfg = new rvn_configuration
                {
                    io_ring_queue_size = PalConfiguration.IoRingQueueSize,
                    low_priority_io = PalConfiguration.LowPriorityIo,
                    pal_version = -1, // loaded by the call
                    version = RvnConfigurationVersion.Current,
                    write_mode = PalConfiguration.WriteMode,
                    memoryLockCallback = &MemoryLockUsage.UpdateLockedMemory,
                    recoveryMemoryLockFailureCallback = &MemoryLockUsage.RecoverLockedMemoryFailure
                };
                rc = rvn_startup_configure(ref cfg, out errorCode);
                if(rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, "Failed to configure PAL library.");
                
                if (cfg.pal_version != PAL_VER)
                {
                    throw new IncorrectDllException(
                        $"{LIBRVNPAL} version '{cfg.pal_version}' mismatches this RavenDB instance version (set to '{PAL_VER}'). Did you forget to set new value in 'rvn_get_pal_ver()'");
                }

                if (PalConfiguration.WriteMode == RvnWriteMode.Auto)
                {
                    // set the actual write mode
                    PalConfiguration.WriteMode = cfg.write_mode;
                }

                if (PalConfiguration.WriteMode != cfg.write_mode)
                {
                    throw new InvalidOperationException($"Requested write mode '{PalConfiguration.WriteMode}', actual write mode was set to '{cfg.write_mode}'");
                }
                
                rc = rvn_get_system_information(out sysInfo, out errorCode);
            }
            catch (Exception ex)
            {
                var errString = $"{LIBRVNPAL} version might be invalid, missing or not usable on current platform.";

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && ex is not IncorrectDllException)
                    errString += 
                        $" Initialization error could also be caused by missing 'Microsoft Visual C++ 2019 Redistributable Package' (or newer). It can be downloaded from '{PlatformDetails.GetVcRedistLink()}'.";

                errString += $" Arch: {RuntimeInformation.OSArchitecture}, OSDesc: {RuntimeInformation.OSDescription}";

                throw new IncorrectDllException(errString, ex);
            }

            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, "Cannot get system information");
            
            PalVoronPageSize = sysInfo.VoronPageSize;
        }

        public static readonly int PalVoronPageSize;

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
        public static extern PalFlags.ErrnoSpecialCodes rvn_get_error_meaning(Int32 error);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_mmap_anonymous(out void* mem,
            UInt64 size,
            out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_mumap_anonymous(void* mem,
            UInt64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_unmap_memory(void* handle,
            void* mem,
            Int64 size,
            out Int32 errorCode);


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_pager_get_file_size(void* handle,
                out Int64 total_size,
                out Int64 phyiscal_size,
                out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_pager_set_sparse_region(void* mem,
            Int64 offset,
            Int64 size,
            out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_map_memory(void* handle,
            Int64 offset,
            Int64 size,
            out void* mem,
            out Int32 errorCode);
            
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_close_pager(
            void* handle, out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_sync_pager(
            void* handle, out Int32 errorCode);

        public static PalFlags.FailCodes rvn_init_pager(
            string filename,
            Int64 initialFileSize,
            OpenFileFlags flags,
            out void* handle,
            out byte* readAddress,
            out byte* writeAddress,
            out Int64 memorySize,
            out Int32 errorCode)
        {
            using var convert = new Converter(filename);
            return rvn_init_pager(convert.Pointer, initialFileSize, flags,
                out handle, out readAddress, out writeAddress, out memorySize, out errorCode);
        }

        [DllImport(LIBRVNPAL, SetLastError = true, CharSet = CharSet.Ansi)]
        public static extern PalFlags.FailCodes rvn_sync_directories(
            void* handle,
            [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[] folders,
            Int32 count,
            out Int32 errorCode);

        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_increase_pager_size(
            void* handle,
            Int64 newFileSize,
            out void* newHandle,
            out byte* readAddress,
            out byte* writeAddress,
            out Int64 memorySize,
            out Int32 errorCode);
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_init_pager(
            byte* filename,
            Int64 initialFileSize,
            OpenFileFlags flags,
            out void* handle,
            out byte* readAddress,
            out byte* writeAddress,
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

        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern PalFlags.FailCodes rvn_get_system_information(
            out PalDefinitions.SystemInformation systemInformation,
            out Int32 errorCode);


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_prefetch_ranges(
            PalDefinitions.PrefetchRanges* list,
            Int32 count,
            out Int32 errorCode);

        [StructLayout(LayoutKind.Sequential)]
        public struct page_to_write : IComparable<page_to_write>
        {
            public Int64 page_num;
            public Int32 count_of_pages;
            public void* ptr;

            public int CompareTo(page_to_write other)
            {
                return page_num.CompareTo(other.page_num);
            }
        };

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        public delegate PalFlags.FailCodes WriterFunc(
            void* handle,
            page_to_write* buffers,
            Int32 count,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern WriterFunc rvn_get_writer(void* handle);

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

        public static PalFlags.FailCodes rvn_is_same_hard_link(string src, string dst, out bool isSame, out Int32 errorCode)
        {
            using var convertSrc = new Converter(src);
            using var convertDst = new Converter(dst);
            return rvn_is_same_hard_link(convertSrc.Pointer, convertDst.Pointer, out isSame, out errorCode);
        }
        
        public static bool rvn_is_same_hard_link(string src, string dst)
        {
            var rc = rvn_is_same_hard_link(src, dst, out var isSame, out var errorCode);
            if (rc != PalFlags.FailCodes.Success)
            {
                PalHelper.ThrowLastError(rc, errorCode, "Failed to check hard link");
            }

            return isSame;
        }
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern PalFlags.FailCodes rvn_is_same_hard_link(
            byte* src, byte* dst, out bool isSame, out Int32 errorCode);

        public static PalFlags.FailCodes rvn_ensure_hard_link_non_durable(string src, string dst, out Int32 errorCode)
        {
            using var convertSrc = new Converter(src);
            using var convertDst = new Converter(dst);
            return rvn_ensure_hard_link_non_durable(convertSrc.Pointer, convertDst.Pointer, out errorCode);
        }
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern PalFlags.FailCodes rvn_ensure_hard_link_non_durable(byte* src, byte* dst, out Int32 errorCode);

        public static PalFlags.FailCodes rvn_hard_link_non_durable(string src, string dst, out Int32 errorCode)
        {
            using var convertSrc = new Converter(src);
            using var convertDst = new Converter(dst);
            return rvn_hard_link_non_durable(convertSrc.Pointer, convertDst.Pointer, out errorCode);
        }


        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern PalFlags.FailCodes rvn_hard_link_non_durable(byte* src, byte* dst, out Int32 errorCode);

        [StructLayout(LayoutKind.Sequential)]
        public struct journal_entry
        {
            public void* Base;
            public Int64 NumberOf4Kbs;
        }

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_write_journal(
            SafeJournalHandle handle,
            journal_entry* entries,
            Int64 countOfEntries,
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

        public enum RvnConfigurationVersion
        {
            None,
            Current
        }
        
        public enum RvnWriteMode
        {
            Auto,
            VectoredFileIo,
            FileIo,
            IoRing,
            Mmap,
        }
        
        [StructLayout(LayoutKind.Sequential)]
        public struct rvn_configuration
        {
            public RvnConfigurationVersion version;
            public Int32 pal_version;
            public Int32 io_ring_queue_size;
            public RvnWriteMode write_mode;
            public bool low_priority_io;

            public delegate* unmanaged[Cdecl]<Int64, char*, void> memoryLockCallback;
            public delegate* unmanaged[Cdecl]<Int64, char*, int> recoveryMemoryLockFailureCallback;
        };
        
        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_startup_configure(ref rvn_configuration cfg, out Int32 errorCode);
        
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
                Encoding encoding = PlatformDetails.RunningOnWindows ? Encoding.Unicode : Encoding.UTF8;
                var size = encoding.GetMaxByteCount(s.Length) + sizeof(char);
                _buffer = ArrayPool<byte>.Shared.Rent(size);
                int length = encoding.GetBytes(s, 0, s.Length, _buffer, 0);
                if (length > size - sizeof(char))
                {
                    throw new InvalidOperationException(
                        $"Invalid length of GetBytes while converting string : '{s}' using '{encoding.EncodingName}' Encoder. Got length of {length} bytes while max size for the string using this encoder is {encoding.GetMaxByteCount(s.Length)}");
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
