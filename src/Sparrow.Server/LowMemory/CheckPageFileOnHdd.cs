//-----------------------------------------------------------------------
// <copyright file="CreateFolderIcon.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Logging;
using Sparrow.Platform.Posix;
using Sparrow.Server.Platform.Win32;

namespace Sparrow.Server.LowMemory
{
    public static class CheckPageFileOnHdd
    {
        private static readonly Logger Log = LoggingSource.Instance.GetLogger("Server", typeof(CheckPageFileOnHdd).FullName);

        private const string PageFileName = "pagefile.sys";

        // ReSharper disable InconsistentNaming
        public static string WindowsIsSwappingOnHddInsteadOfSsd()
        {
            try
            {
                var drives = DriveInfo.GetDrives()
                    .Where(x => x.DriveType == DriveType.Fixed)
                    .ToList();

                var hddDrivesWithPageFile = new List<string>();
                var ssdDriveCount = 0;
                for (var i = 0; i < drives.Count; i++)
                {
                    var fullDriveName = drives[i].RootDirectory.FullName;
                    var currentDriveLetter = fullDriveName.Substring(0, 1);
                    var driveNumber = GetPhysicalDriveNumber(currentDriveLetter);
                    if (driveNumber == null)
                        continue;

                    var driveType = GetDriveType(driveNumber.Value);
                    switch (driveType)
                    {
                        case RavenDriveType.SSD:
                            ssdDriveCount++;
                            continue;
                        case RavenDriveType.HDD:
                            break;

                        case RavenDriveType.Unknown:
                            if (Log.IsOperationsEnabled)
                                Log.Operations($"Failed to determine if drive {currentDriveLetter} is SSD or HDD");
                            //we can't figure out the drive type
                            continue;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    if (File.Exists($"{fullDriveName}{PageFileName}") == false)
                        continue;

                    hddDrivesWithPageFile.Add(currentDriveLetter);
                }

                if (ssdDriveCount > 0 && hddDrivesWithPageFile.Count == 0 ||
                    hddDrivesWithPageFile.Count == 0)
                {
                    // the system has ssd drives and has no hdd drives with a page file on them
                    // or no hdd drive with page file at all
                    return null;
                }

                Debug.Assert(hddDrivesWithPageFile.Count > 0); // RavenDB-

                var message = $"A page file was found on HDD drive{(hddDrivesWithPageFile.Count > 1 ? "s" : string.Empty)}: " +
                              $"{string.Join(", ", hddDrivesWithPageFile)} while there is {ssdDriveCount} " +
                              $"SSD drive{(ssdDriveCount > 1 ? "s" : string.Empty)}. This can cause a slowdown, consider moving it to SSD";

                if (Log.IsInfoEnabled)
                    Log.Info(message);

                return string.Join(", ", hddDrivesWithPageFile);
            }
            catch (Exception e)
            {
                if (Log.IsInfoEnabled)
                    Log.Info("Failed to determine page file that is located on HDD", e);
                return null;
            }
        }

        private enum RavenDriveType
        {
            SSD = 0,
            HDD = 1,
            Unknown
        }

        private static RavenDriveType GetDriveType(uint physicalDriveNumber)
        {
            var sDrive = "\\\\.\\PhysicalDrive" + physicalDriveNumber;
            var driveType = HasNoSeekPenalty(sDrive);
            return driveType != RavenDriveType.Unknown ? driveType : HasNominalMediaRotationRate(sDrive);
        }

        //for CreateFile to get handle to drive
        private const uint GENERIC_READ = 0x80000000;

        private const uint GENERIC_WRITE = 0x40000000;
        private const uint FILE_SHARE_READ = 0x00000001;
        private const uint FILE_SHARE_WRITE = 0x00000002;
        private const uint OPEN_EXISTING = 3;
        private const uint FILE_ATTRIBUTE_NORMAL = 0x00000080;

        //createFile to get handle to drive
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateFileW([MarshalAs(UnmanagedType.LPWStr)] string lpFileName, uint dwDesiredAccess, uint dwShareMode, IntPtr lpSecurityAttributes, uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

        //for control codes
        private const uint FILE_DEVICE_MASS_STORAGE = 0x0000002d;

        private const uint IOCTL_STORAGE_BASE = FILE_DEVICE_MASS_STORAGE;
        private const uint FILE_DEVICE_CONTROLLER = 0x00000004;
        private const uint IOCTL_SCSI_BASE = FILE_DEVICE_CONTROLLER;
        private const uint METHOD_BUFFERED = 0;
        private const uint FILE_ANY_ACCESS = 0;
        private const uint FILE_READ_ACCESS = 0x00000001;
        private const uint FILE_WRITE_ACCESS = 0x00000002;
        private const uint IOCTL_VOLUME_BASE = 0x00000056;

        private static uint CTL_CODE(uint DeviceType, uint Function, uint Method, uint Access)
        {
            return ((DeviceType << 16) | (Access << 14) | (Function << 2) | Method);
        }

        //for DeviceIoControl to check no seek penalty
        private const uint StorageDeviceSeekPenaltyProperty = 7;

        private const uint PropertyStandardQuery = 0;

        [StructLayout(LayoutKind.Sequential)]
        private struct STORAGE_PROPERTY_QUERY
        {
            public uint PropertyId;
            public uint QueryType;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)] public byte[] AdditionalParameters;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct DEVICE_SEEK_PENALTY_DESCRIPTOR
        {
            public uint Version;
            public uint Size;
            [MarshalAs(UnmanagedType.U1)] public bool IncursSeekPenalty;
        }

        //deviceIoControl to check no seek penalty
        [DllImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DeviceIoControl(IntPtr hDevice, uint dwIoControlCode, ref STORAGE_PROPERTY_QUERY lpInBuffer, uint nInBufferSize, ref DEVICE_SEEK_PENALTY_DESCRIPTOR lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

        //for DeviceIoControl to check nominal media rotation rate
        private const uint ATA_FLAGS_DATA_IN = 0x02;

        [StructLayout(LayoutKind.Sequential)]
        private struct ATA_PASS_THROUGH_EX
        {
            public ushort Length;
            public ushort AtaFlags;
            private readonly byte PathId;
            private readonly byte TargetId;
            private readonly byte Lun;
            private readonly byte ReservedAsUchar;
            public uint DataTransferLength;
            public uint TimeOutValue;
            private readonly uint ReservedAsUlong;
            public IntPtr DataBufferOffset;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)] public byte[] PreviousTaskFile;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)] public byte[] CurrentTaskFile;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ATAIdentifyDeviceQuery
        {
            public ATA_PASS_THROUGH_EX header;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 256)] public ushort[] data;
        }

        //deviceIoControl to check nominal media rotation rate
        [DllImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DeviceIoControl(IntPtr hDevice, uint dwIoControlCode, ref ATAIdentifyDeviceQuery lpInBuffer, uint nInBufferSize, ref ATAIdentifyDeviceQuery lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

        //for error message
        private const uint FORMAT_MESSAGE_FROM_SYSTEM = 0x00001000;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint FormatMessage(uint dwFlags, IntPtr lpSource, uint dwMessageId, uint dwLanguageId, StringBuilder lpBuffer, uint nSize, IntPtr Arguments);

        private static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        //method for no seek penalty
        private static RavenDriveType HasNoSeekPenalty(string sDrive)
        {
            var hDrive = CreateFileW(sDrive, 0, // No access to drive
                FILE_SHARE_READ | FILE_SHARE_WRITE, IntPtr.Zero, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);

            DEVICE_SEEK_PENALTY_DESCRIPTOR querySeekPenaltyDesc;
            bool querySeekPenaltyResult;
            try
            {
                if (hDrive == INVALID_HANDLE_VALUE)
                {
                    var message = GetErrorMessage(Marshal.GetLastWin32Error());
                    if (Log.IsInfoEnabled)
                        Log.Info("CreateFile failed. " + message);
                    return RavenDriveType.Unknown;
                }

                var IOCTL_STORAGE_QUERY_PROPERTY = CTL_CODE(IOCTL_STORAGE_BASE, 0x500, METHOD_BUFFERED, FILE_ANY_ACCESS); // From winioctl.h

                var query_seek_penalty = new STORAGE_PROPERTY_QUERY
                {
                    PropertyId = StorageDeviceSeekPenaltyProperty,
                    QueryType = PropertyStandardQuery
                };

                querySeekPenaltyDesc = new DEVICE_SEEK_PENALTY_DESCRIPTOR();

                uint returnedQuerySeekPenaltySize;

                querySeekPenaltyResult = DeviceIoControl(hDrive, IOCTL_STORAGE_QUERY_PROPERTY, ref query_seek_penalty, (uint)Marshal.SizeOf(query_seek_penalty), ref querySeekPenaltyDesc, (uint)Marshal.SizeOf(querySeekPenaltyDesc), out returnedQuerySeekPenaltySize, IntPtr.Zero);
            }
            finally
            {
                Win32ThreadsMethods.CloseHandle(hDrive);
            }

            if (querySeekPenaltyResult == false)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());
                if (Log.IsInfoEnabled)
                    Log.Info("DeviceIoControl failed. " + message);
                return RavenDriveType.Unknown;
            }

            return querySeekPenaltyDesc.IncursSeekPenalty == false ? RavenDriveType.SSD : RavenDriveType.HDD;
        }

        //method for nominal media rotation rate
        //(administrative privilege is required)
        private static RavenDriveType HasNominalMediaRotationRate(string sDrive)
        {
            var hDrive = CreateFileW(sDrive, GENERIC_READ | GENERIC_WRITE, //administrative privilege is required
                FILE_SHARE_READ | FILE_SHARE_WRITE, IntPtr.Zero, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);

            ATAIdentifyDeviceQuery id_query;
            bool result;
            try
            {
                if (hDrive == INVALID_HANDLE_VALUE)
                {
                    var message = GetErrorMessage(Marshal.GetLastWin32Error());
                    if (Log.IsInfoEnabled)
                        Log.Info("CreateFile failed. " + message);
                    return RavenDriveType.Unknown;
                }

                var ioctlAtaPassThrough = CTL_CODE(IOCTL_SCSI_BASE, 0x040b, METHOD_BUFFERED, FILE_READ_ACCESS | FILE_WRITE_ACCESS); // From ntddscsi.h

                id_query = new ATAIdentifyDeviceQuery();
                id_query.data = new ushort[256];

                id_query.header.Length = (ushort)Marshal.SizeOf(id_query.header);
                id_query.header.AtaFlags = (ushort)ATA_FLAGS_DATA_IN;
                id_query.header.DataTransferLength = (uint)(id_query.data.Length * 2); // Size of "data" in bytes
                id_query.header.TimeOutValue = 3; // Sec
#pragma warning disable 618
                id_query.header.DataBufferOffset = Marshal.OffsetOf(typeof(ATAIdentifyDeviceQuery), "data");
#pragma warning restore 618
                id_query.header.PreviousTaskFile = new byte[8];
                id_query.header.CurrentTaskFile = new byte[8];
                id_query.header.CurrentTaskFile[6] = 0xec; // ATA IDENTIFY DEVICE

                uint retvalSize;

                result = DeviceIoControl(hDrive, ioctlAtaPassThrough, ref id_query, (uint)Marshal.SizeOf(id_query), ref id_query, (uint)Marshal.SizeOf(id_query),
                    out retvalSize, IntPtr.Zero);
            }
            finally
            {
                Win32ThreadsMethods.CloseHandle(hDrive);
            }

            if (result == false)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());
                if (Log.IsInfoEnabled)
                    Log.Info("DeviceIoControl failed. " + message);
                return RavenDriveType.Unknown;
            }

            //word index of nominal media rotation rate
            //(1 means non-rotate device)
            const int kNominalMediaRotRateWordIndex = 217;
            return id_query.data[kNominalMediaRotRateWordIndex] == 1 ? RavenDriveType.SSD : RavenDriveType.HDD;
        }

        //method for error message
        private static string GetErrorMessage(int code)
        {
            var message = new StringBuilder(255);

            FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, IntPtr.Zero, (uint)code, 0, message, (uint)message.Capacity, IntPtr.Zero);

            return message.ToString();
        }

        //for DeviceIoControl to get disk extents
        [StructLayout(LayoutKind.Sequential)]
        private struct DISK_EXTENT
        {
            public readonly uint DiskNumber;
            private readonly long StartingOffset;
            private readonly long ExtentLength;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct VOLUME_DISK_EXTENTS
        {
            private readonly uint NumberOfDiskExtents;
            [MarshalAs(UnmanagedType.ByValArray)] public readonly DISK_EXTENT[] Extents;
        }

        // DeviceIoControl to get disk extents
        [DllImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DeviceIoControl(IntPtr hDevice, uint dwIoControlCode, IntPtr lpInBuffer, uint nInBufferSize, ref VOLUME_DISK_EXTENTS lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

        //method for disk extents
        public static uint? GetPhysicalDriveNumber(string driveLetter)
        {
            var sDrive = "\\\\.\\" + driveLetter + ":";

            var hDrive = CreateFileW(sDrive, 0, // No access to drive
                FILE_SHARE_READ | FILE_SHARE_WRITE, IntPtr.Zero, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);

            VOLUME_DISK_EXTENTS query_disk_extents;
            bool query_disk_extents_result;
            try
            {
                if (hDrive == INVALID_HANDLE_VALUE)
                {
                    var message = GetErrorMessage(Marshal.GetLastWin32Error());
                    if (Log.IsInfoEnabled)
                        Log.Info("CreateFile failed. " + message);
                    return uint.MinValue;
                }

                uint IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS = CTL_CODE(IOCTL_VOLUME_BASE, 0, METHOD_BUFFERED, FILE_ANY_ACCESS); // From winioctl.h

                query_disk_extents = new VOLUME_DISK_EXTENTS();

                uint returned_query_disk_extents_size;

                query_disk_extents_result = DeviceIoControl(hDrive, IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS, IntPtr.Zero, 0, ref query_disk_extents,
                    (uint)Marshal.SizeOf(query_disk_extents), out returned_query_disk_extents_size, IntPtr.Zero);
            }
            finally
            {
                Win32ThreadsMethods.CloseHandle(hDrive);
            }

            if (query_disk_extents_result == false || query_disk_extents.Extents.Length != 1)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());
                if (Log.IsInfoEnabled)
                    Log.Info("DeviceIoControl failed. " + message);
                return null;
            }

            return query_disk_extents.Extents[0].DiskNumber;
        }

        public static string PosixIsSwappingOnHddInsteadOfSsd()
        {
            try
            {
                var swaps = KernelVirtualFileSystemUtils.ReadSwapInformationFromSwapsFile();
                if (swaps.Length == 0) // on error return as if no swap problem
                    return null;

                const string sysBlockDirectoryPath = "/sys/block/";

                var blocks = Directory
                    .GetDirectories(sysBlockDirectoryPath)
                    .Select(x => x.Substring(sysBlockDirectoryPath.Length))
                    .Where(x => x.StartsWith("loop") == false)
                    .ToList();

                if (blocks.Count == 0)
                    return null;

                string foundRotationalDiskDrive = null;
                for (int i = 0; i < swaps.Length; i++)
                {
                    if (swaps[i].IsDeviceSwapFile)
                        continue; // we do not check swap file, only partitions

                    if (TryFindDisk(swaps[i].DeviceName, out var disk) == false)
                        continue;

                    var filename = $"/sys/block/{disk}/queue/rotational";
                    if (File.Exists(filename) == false)
                        continue;

                    var isHdd = KernelVirtualFileSystemUtils.ReadNumberFromFile(filename);
                    if (isHdd == -1)
                        return null;
                    if (isHdd == 1)
                        foundRotationalDiskDrive = filename;
                    else if (isHdd != 0)
                    {
                        if (Log.IsOperationsEnabled)
                            Log.Operations($"Got invalid value (not 0 or 1) from {filename} = {isHdd}, assumes this is not a rotational disk");
                        foundRotationalDiskDrive = null;
                    }
                }

                string hddSwapsInsteadOfSsd = null;
                if (foundRotationalDiskDrive != null)
                {
                    // search if ssd drive is available
                    foreach (var partitionDisk in KernelVirtualFileSystemUtils.GetAllDisksFromPartitionsFile())
                    {
                        //ignore ramdisks (ram0..ram15 etc)
                        if (partitionDisk.StartsWith("ram", StringComparison.OrdinalIgnoreCase))
                            continue;

                        if (TryFindDisk(partitionDisk, out var disk) == false)
                            continue;

                        var filename = $"/sys/block/{disk}/queue/rotational";
                        if (File.Exists(filename) == false)
                            continue;

                        var isHdd = KernelVirtualFileSystemUtils.ReadNumberFromFile(filename);
                        if (isHdd == 0)
                        {
                            hddSwapsInsteadOfSsd = disk;
                            break;
                        }
                    }
                }

                return hddSwapsInsteadOfSsd;

                bool TryFindDisk(string deviceName, out string disk)
                {
                    disk = null;

                    if (string.IsNullOrWhiteSpace(deviceName))
                        return false;

                    deviceName = deviceName.Replace("/dev/", string.Empty);

                    foreach (var block in blocks)
                    {
                        if (deviceName.Contains(block))
                        {
                            disk = block;
                            return true;
                        }
                    }

                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Info("Error while trying to determine if hdd swaps instead of ssd on linux, ignoring this check and assuming no hddswap", ex);
                // ignore
                return null;
            }
        }
    }
}
