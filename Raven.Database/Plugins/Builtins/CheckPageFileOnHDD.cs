//-----------------------------------------------------------------------
// <copyright file="CreateFolderIcon.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Logging;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server;

namespace Raven.Database.Plugins.Builtins
{
    public class CheckPageFileOnHdd : IServerStartupTask
    {
        private static ILog log = LogManager.GetCurrentClassLogger();
        private const string PageFileName = "pagefile.sys";

        public void Execute(RavenDBOptions serverOptions)
        {
            if (serverOptions.Disposed)
            {
                Dispose();
                return;
            }

            try
            {
                var drives = DriveInfo.GetDrives()
                    .Where(x => x.DriveType == System.IO.DriveType.Fixed)
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
                        case DriveType.SSD:
                            ssdDriveCount++;
                            continue;
                        case DriveType.HDD:
                            break;
                        case DriveType.Unknown:
                            //we can't figure out the drive type
                            if (log.IsDebugEnabled)
                                log.Debug($"Failed to determine if drive {currentDriveLetter} is SSD or HDD");

                            continue;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    if (File.Exists($"{fullDriveName}{PageFileName}") == false)
                        continue;

                    hddDrivesWithPageFile.Add(currentDriveLetter);
                }

                if (ssdDriveCount == 0 || hddDrivesWithPageFile.Count == 0)
                {
                    //the system has no ssd drives or has no hdd drives with a page file on them
                    return;
                }

                var message = $"A page file was found on HDD drive{(hddDrivesWithPageFile.Count > 1 ? "s" : string.Empty)}: " +
                              $"{string.Join(", ", hddDrivesWithPageFile)} while there {(ssdDriveCount > 1 ? "are" : "is")} {ssdDriveCount} " +
                              $"SSD drive{(ssdDriveCount > 1 ? "s" : string.Empty)}. This can cause a slowdown, consider moving it to SSD";

                log.Warn(message);

                serverOptions.SystemDatabase.AddAlert(new Alert
                {
                    AlertLevel = AlertLevel.Warning,
                    CreatedAt = SystemTime.UtcNow,
                    Title = message,
                    UniqueKey = "Page file was found on HDD drive"
                });
            }
            catch (Exception e)
            {
                log.WarnException("Failed to determine whether the page file is located on HDD", e);
            }
        }

        public void Dispose() { }

        private enum DriveType
        {
            SSD = 0,
            HDD = 1,
            Unknown
        }

        private static DriveType GetDriveType(uint physicalDriveNumber)
        {
            var sDrive = "\\\\.\\PhysicalDrive" + physicalDriveNumber;
            var driveType = HasNoSeekPenalty(sDrive);
            return driveType != DriveType.Unknown ? driveType : HasNominalMediaRotationRate(sDrive);
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
        private static extern SafeFileHandle CreateFileW([MarshalAs(UnmanagedType.LPWStr)] string lpFileName, uint dwDesiredAccess, uint dwShareMode, IntPtr lpSecurityAttributes, uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

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
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public byte[] AdditionalParameters;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct DEVICE_SEEK_PENALTY_DESCRIPTOR
        {
            public uint Version;
            public uint Size;
            [MarshalAs(UnmanagedType.U1)]
            public bool IncursSeekPenalty;
        }

        //deviceIoControl to check no seek penalty
        [DllImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DeviceIoControl(SafeFileHandle hDevice, uint dwIoControlCode, ref STORAGE_PROPERTY_QUERY lpInBuffer, uint nInBufferSize, ref DEVICE_SEEK_PENALTY_DESCRIPTOR lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

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
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public byte[] PreviousTaskFile;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public byte[] CurrentTaskFile;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ATAIdentifyDeviceQuery
        {
            public ATA_PASS_THROUGH_EX header;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 256)]
            public ushort[] data;
        }

        //deviceIoControl to check nominal media rotation rate
        [DllImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DeviceIoControl(SafeFileHandle hDevice, uint dwIoControlCode, ref ATAIdentifyDeviceQuery lpInBuffer, uint nInBufferSize, ref ATAIdentifyDeviceQuery lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

        //for error message
        private const uint FORMAT_MESSAGE_FROM_SYSTEM = 0x00001000;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint FormatMessage(uint dwFlags, IntPtr lpSource, uint dwMessageId, uint dwLanguageId, StringBuilder lpBuffer, uint nSize, IntPtr Arguments);

        //method for no seek penalty
        private static DriveType HasNoSeekPenalty(string sDrive)
        {
            var hDrive = CreateFileW(sDrive, 0, // No access to drive
                FILE_SHARE_READ | FILE_SHARE_WRITE, IntPtr.Zero, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);

            if (hDrive == null || hDrive.IsInvalid)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());

                if (log.IsDebugEnabled)
                    log.Debug("CreateFile failed. " + message);

                return DriveType.Unknown;
            }

            var IOCTL_STORAGE_QUERY_PROPERTY = CTL_CODE(IOCTL_STORAGE_BASE, 0x500, METHOD_BUFFERED, FILE_ANY_ACCESS); // From winioctl.h

            var query_seek_penalty = new STORAGE_PROPERTY_QUERY
            {
                PropertyId = StorageDeviceSeekPenaltyProperty,
                QueryType = PropertyStandardQuery
            };

            var querySeekPenaltyDesc = new DEVICE_SEEK_PENALTY_DESCRIPTOR();

            uint returnedQuerySeekPenaltySize;

            var querySeekPenaltyResult = DeviceIoControl(hDrive, IOCTL_STORAGE_QUERY_PROPERTY, ref query_seek_penalty, (uint)Marshal.SizeOf(query_seek_penalty), ref querySeekPenaltyDesc, (uint)Marshal.SizeOf(querySeekPenaltyDesc), out returnedQuerySeekPenaltySize, IntPtr.Zero);

            hDrive.Close();

            if (querySeekPenaltyResult == false)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());

                if (log.IsDebugEnabled)
                    log.Debug("DeviceIoControl failed. " + message);

                return DriveType.Unknown;
            }

            return querySeekPenaltyDesc.IncursSeekPenalty == false ? DriveType.SSD : DriveType.HDD;
        }

        //method for nominal media rotation rate
        //(administrative privilege is required)
        private static DriveType HasNominalMediaRotationRate(string sDrive)
        {
            var hDrive = CreateFileW(sDrive, GENERIC_READ | GENERIC_WRITE, //administrative privilege is required
                FILE_SHARE_READ | FILE_SHARE_WRITE, IntPtr.Zero, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);

            if (hDrive == null || hDrive.IsInvalid)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());

                if (log.IsDebugEnabled)
                    log.Debug("CreateFile failed. " + message);

                return DriveType.Unknown;
            }

            var ioctlAtaPassThrough = CTL_CODE(IOCTL_SCSI_BASE, 0x040b, METHOD_BUFFERED, FILE_READ_ACCESS | FILE_WRITE_ACCESS); // From ntddscsi.h

            var id_query = new ATAIdentifyDeviceQuery();
            id_query.data = new ushort[256];

            id_query.header.Length = (ushort)Marshal.SizeOf(id_query.header);
            id_query.header.AtaFlags = (ushort)ATA_FLAGS_DATA_IN;
            id_query.header.DataTransferLength = (uint)(id_query.data.Length * 2); // Size of "data" in bytes
            id_query.header.TimeOutValue = 3; // Sec
            id_query.header.DataBufferOffset = (IntPtr)Marshal.OffsetOf(typeof(ATAIdentifyDeviceQuery), "data");
            id_query.header.PreviousTaskFile = new byte[8];
            id_query.header.CurrentTaskFile = new byte[8];
            id_query.header.CurrentTaskFile[6] = 0xec; // ATA IDENTIFY DEVICE

            uint retvalSize;

            var result = DeviceIoControl(hDrive, ioctlAtaPassThrough, ref id_query, (uint)Marshal.SizeOf(id_query), ref id_query, (uint)Marshal.SizeOf(id_query), out retvalSize, IntPtr.Zero);

            hDrive.Close();

            if (result == false)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());

                if (log.IsDebugEnabled)
                    log.Debug("DeviceIoControl failed. " + message);

                return DriveType.Unknown;
            }

            //word index of nominal media rotation rate
            //(1 means non-rotate device)
            const int kNominalMediaRotRateWordIndex = 217;
            return id_query.data[kNominalMediaRotRateWordIndex] == 1 ? DriveType.SSD : DriveType.HDD;
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
            [MarshalAs(UnmanagedType.ByValArray)]
            public readonly DISK_EXTENT[] Extents;
        }

        // DeviceIoControl to get disk extents
        [DllImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DeviceIoControl(SafeFileHandle hDevice, uint dwIoControlCode, IntPtr lpInBuffer, uint nInBufferSize, ref VOLUME_DISK_EXTENTS lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

        //method for disk extents
        private static uint? GetPhysicalDriveNumber(string driveLetter)
        {
            var sDrive = "\\\\.\\" + driveLetter + ":";

            var hDrive = CreateFileW(sDrive, 0, // No access to drive
                FILE_SHARE_READ | FILE_SHARE_WRITE, IntPtr.Zero, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);

            if (hDrive == null || hDrive.IsInvalid)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());

                if (log.IsDebugEnabled)
                    log.Debug("CreateFile failed. " + message);

                return uint.MinValue;
            }

            uint IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS = CTL_CODE(IOCTL_VOLUME_BASE, 0, METHOD_BUFFERED, FILE_ANY_ACCESS); // From winioctl.h

            VOLUME_DISK_EXTENTS query_disk_extents = new VOLUME_DISK_EXTENTS();

            uint returned_query_disk_extents_size;

            bool query_disk_extents_result = DeviceIoControl(hDrive, IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS, IntPtr.Zero, 0, ref query_disk_extents, (uint)Marshal.SizeOf(query_disk_extents), out returned_query_disk_extents_size, IntPtr.Zero);

            hDrive.Close();

            if (query_disk_extents_result == false || query_disk_extents.Extents.Length != 1)
            {
                var message = GetErrorMessage(Marshal.GetLastWin32Error());

                if (log.IsDebugEnabled)
                    log.Debug("DeviceIoControl failed. " + message);

                return null;
            }

            return query_disk_extents.Extents[0].DiskNumber;
        }
    }
}
