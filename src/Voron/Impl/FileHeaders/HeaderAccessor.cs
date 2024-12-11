// -----------------------------------------------------------------------
//  <copyright file="HeaderAccessor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Voron.Schema;
using Voron.Util;

namespace Voron.Impl.FileHeaders
{
    public delegate void ModifyHeaderAction(ref FileHeader header);

    public delegate T GetDataFromHeaderAction<T>(in FileHeader header);

    public sealed unsafe class HeaderAccessor(StorageEnvironment env) : IDisposable
    {
        private readonly ReaderWriterLockSlim _locker = new();
        private long _revision;

        private FileHeader _theHeader;
        private bool _disposed;

        internal static readonly string[] HeaderFileNames = { "headers.one", "headers.two" };

        public bool Initialize()
        {
            _locker.EnterWriteLock();
            try
            {
                if (_disposed)
                    throw new ObjectDisposedException("Cannot access the header after it was disposed");

                var hasOne = env.Options.ReadHeader(HeaderFileNames[0], out var headerOne);
                var hasTwo = env.Options.ReadHeader(HeaderFileNames[1], out var headerTwo);
                if (hasOne  is false && hasTwo is false)
                {
                    // new 
                    FillInEmptyHeader(ref headerOne);
                    headerOne.Hash = CalculateFileHeaderHash(MemoryMarshal.AsBytes(new Span<FileHeader>(ref headerOne)), headerOne.TransactionId);
                    env.Options.WriteHeader(HeaderFileNames[0], headerOne);
                    env.Options.WriteHeader(HeaderFileNames[1], headerOne);

                    _theHeader = headerOne;
                    return true; // new
                }

                if (headerOne.MagicMarker != Constants.MagicMarker && headerTwo.MagicMarker != Constants.MagicMarker)
                    throw new InvalidDataException("None of the header files start with the magic marker, probably not db files or fatal corruption on " + env.Options.BasePath);

                var headerOneBytes = MemoryMarshal.AsBytes(new System.Span<FileHeader>(ref headerOne));
                var headerOneValid = headerOne.Hash == CalculateFileHeaderHash(headerOneBytes, headerOne.TransactionId);
                var headerTwoBytes = MemoryMarshal.AsBytes(new System.Span<FileHeader>(ref headerTwo));
                var headerTwoValid = headerTwo.Hash == CalculateFileHeaderHash(headerTwoBytes, headerTwo.TransactionId);

                if (headerOneValid is false && headerTwoValid is false)
                    throw new InvalidDataException("None of the header files have a valid hash, possible corruption on " + env.Options.BasePath);

                // if one of the files is corrupted, but the other isn't, restore to the valid file
                if (headerOne.MagicMarker != Constants.MagicMarker || headerOneValid is false)
                {
                    headerOne = headerTwo;
                }
                
                if (headerTwo.MagicMarker != Constants.MagicMarker || headerTwoValid is false)
                {
                    headerTwo = headerOne;
                }

                if (headerOne.TransactionId < 0)
                    throw new InvalidDataException("The transaction number cannot be negative on " + env.Options.BasePath);

                if (headerOne.HeaderRevision > headerTwo.HeaderRevision)
                {
                    _theHeader = headerOne;
                }
                else
                {
                    _theHeader = headerTwo;
                }
                _revision = _theHeader.HeaderRevision;

                if (_theHeader.Version != Constants.CurrentVersion)
                {
                    _locker.ExitWriteLock();
                    try
                    {
                        var updater = new VoronSchemaUpdater(this, env.Options);

                        updater.Update();
                    }
                    finally
                    {
                        _locker.EnterWriteLock();

                    }

                    if (_theHeader.Version != Constants.CurrentVersion)
                    {
                        throw new SchemaErrorException(
                            $"The db file is for version {_theHeader.Version}, which is not compatible with the current version {Constants.CurrentVersion} on {env.Options.BasePath}");
                    }
                }

                if (_theHeader.PageSize != Constants.Storage.PageSize)
                {
                    var message =
                        $"PageSize mismatch, configured to be {Constants.Storage.PageSize:#,#} but was {_theHeader.PageSize:#,#}, " +
                        $"using the actual value in the file {_theHeader.PageSize:#,#}";
                    env.Options.InvokeRecoveryError(this, message, null);
                }
                return IsEmptyHeader(_theHeader);
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }


        public FileHeader CopyHeader()
        {
            _locker.EnterReadLock();
            try
            {
                if (_disposed)
                    throw new ObjectDisposedException("Cannot access the header after it was disposed");
                return _theHeader;
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }


        public T Get<T>(GetDataFromHeaderAction<T> action)
        {
            _locker.EnterReadLock();
            try
            {
                if (_disposed)
                    throw new ObjectDisposedException("Cannot access the header after it was disposed");

                return action(in _theHeader);
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        public void Modify(ModifyHeaderAction modifyAction)
        {
            _locker.EnterWriteLock();
            try
            {
                if (_disposed)
                    throw new ObjectDisposedException("Cannot access the header after it was disposed");

                modifyAction(ref _theHeader);
          
                _revision++;
                _theHeader.HeaderRevision = _revision;

                var file = HeaderFileNames[_revision & 1];

                _theHeader.Hash = CalculateFileHeaderHash(
                    MemoryMarshal.AsBytes(new Span<FileHeader>(ref _theHeader)),
                    _theHeader.TransactionId);
                env.Options.WriteHeader(file, _theHeader);
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }

        private void FillInEmptyHeader(ref FileHeader header)
        {
            header.MagicMarker = Constants.MagicMarker;
            header.Version = Constants.CurrentVersion;
            header.HeaderRevision = -1;
            header.TransactionId = 0;
            header.LastPageNumber = 1;
            header.Root.RootPageNumber = -1;
            header.Journal.CurrentJournal = -1;
            for (int i = 0; i < JournalInfo.NumberOfReservedBytes; i++)
            {
                header.Journal.Reserved[i] = 0;
            }
            header.Journal.Flags = Journal.JournalInfoFlags.None;
            header.Journal.LastSyncedJournal = -1;
            header.Journal.LastSyncedTransactionId = -1;
            header.IncrementalBackup.LastBackedUpJournal = -1;
            header.IncrementalBackup.LastBackedUpJournalPage = -1;
            header.IncrementalBackup.LastCreatedJournal = -1;
            header.PageSize = env.Options.PageSize;
            header.DatabaseId=Guid.Empty;
        }

        private bool IsEmptyHeader(in FileHeader header)
        {
            if (header.MagicMarker != Constants.MagicMarker ||
                header.Version != Constants.CurrentVersion ||
                header.HeaderRevision != -1 ||
                header.TransactionId != 0) 
                return false;
            return header is { LastPageNumber: 1, Root.RootPageNumber: -1, Journal: { CurrentJournal: -1, Flags: JournalInfoFlags.None } } &&
                   header.Journal.Reserved[0] == 0 && 
                   header.Journal.Reserved[1] == 0 && 
                   header.Journal.Reserved[2] == 0 &&
                   header.Journal is { LastSyncedJournal: -1, LastSyncedTransactionId: -1 } &&
                   header.IncrementalBackup.LastBackedUpJournal == -1 &&
                   header.IncrementalBackup is { LastBackedUpJournalPage: -1, LastCreatedJournal: -1 } && 
                   header.DatabaseId == Guid.Empty;
        }

   
        public static ulong CalculateFileHeaderHash(Span<byte> header, long transactionId)
        {
            var ctx = new Hashing.Streamed.XXHash64Context
            {
                Seed = (ulong)transactionId
            };
            Hashing.Streamed.XXHash64.Begin(ref ctx);

            fixed (byte* p = header)
            {
                // First part of header, until the Hash field
                Hashing.Streamed.XXHash64.Process(ref ctx, p, FileHeader.HashOffset);

                // Second part of header, after the hash field
                var secondPartOfHeaderLength = header.Length - (FileHeader.HashOffset + sizeof(ulong));
                if (secondPartOfHeaderLength > 0)
                    Hashing.Streamed.XXHash64.Process(ref ctx, p + FileHeader.HashOffset + sizeof(ulong), secondPartOfHeaderLength);

            }
            return Hashing.Streamed.XXHash64.End(ref ctx);
        }

        public JournalInfo CopyHeaders(BackupZipArchive package, DataCopier copier, StorageEnvironmentOptions envOptions, string basePath)
        {
            _locker.EnterReadLock(); //race between reading the headers while modifying them
            try
            {
                if (_disposed)
                    throw new ObjectDisposedException("Cannot access the header after it was disposed");
                var success = false;
                foreach (var headerFileName in HeaderFileNames)
                {
                    if (envOptions.ReadHeader(headerFileName, out var header) == false)
                        continue;

                    success = true;

                    var headerPart = package.CreateEntry(Path.Combine(basePath, headerFileName));
                    Debug.Assert(headerPart != null);

                    using (var headerStream = headerPart.Open())
                    {
                        copier.ToStream((byte*)&header, sizeof(FileHeader), headerStream);
                    }
                }

                if (!success)
                    throw new InvalidDataException($"Failed to read both file headers (headers.one & headers.two) from path: {basePath}, possible corruption.");

                return _theHeader.Journal;
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }
        
        public void Dispose()
        {
            _locker.EnterWriteLock();
            try
            {
                _disposed = true;
                _theHeader = default;
                _revision = -1;
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }
    }
}
