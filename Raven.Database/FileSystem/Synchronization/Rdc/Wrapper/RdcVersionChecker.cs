using System;
using System.Diagnostics;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged;

namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper
{
    public class RdcVersionChecker : CriticalFinalizerObject, IDisposable
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly ReaderWriterLockSlim _disposerLock = new ReaderWriterLockSlim();

        private IRdcLibrary _rdcLibrary;
        private bool _disposed;

        public RdcVersionChecker()
        {
            try
            {
                _rdcLibrary = (IRdcLibrary)new RdcLibrary();
            }
            catch (InvalidCastException e)
            {
                throw new InvalidOperationException("This code must run in an MTA thread", e);
            }
            catch (COMException comException)
            {
                log.ErrorException("Remote Differential Compression feature is not installed", comException);
                throw new NotSupportedException("Remote Differential Compression feature is not installed", comException);
            }
        }

        public void Dispose()
        {
            _disposerLock.EnterWriteLock();
            try
            {
                if (_disposed)
                    return;
                GC.SuppressFinalize(this);
                DisposeInternal();
            }
            finally
            {
                _disposed = true;
                _disposerLock.ExitWriteLock();
            }
        }

        public RdcVersion GetRdcVersion()
        {
            uint currentVersion, minimumCompatibleAppVersion;
            var hr = _rdcLibrary.GetRDCVersion(out currentVersion, out minimumCompatibleAppVersion);
            if (hr != 0)
            {
                throw new RdcException("Failed to get the rdc version", hr);
            }

            if (currentVersion >= (uint)int.MaxValue)
                throw new InvalidCastException("The CurrentVersion is higher than int.MaxValue. This shouldn't happen. If it happens we have bigger problems.");

            if (minimumCompatibleAppVersion >= (uint)int.MaxValue)
                throw new InvalidCastException("The minimumCompatibleAppVersion is higher than int.MaxValue. This shouldn't happen. If it happens we have bigger problems.");

            return new RdcVersion 
            { 
                CurrentVersion = (int)currentVersion, 
                MinimumCompatibleAppVersion = (int)minimumCompatibleAppVersion 
            };
        }

        private void DisposeInternal()
        {
            if (_rdcLibrary != null)
            {
                Marshal.ReleaseComObject(_rdcLibrary);
                _rdcLibrary = null;
            }
        }
        ~RdcVersionChecker()
        {
            try
            {
                Trace.WriteLine(
                    "~RdcVersionChecker: Disposing COM resources from finalizer! You should call Dispose() instead!");
                DisposeInternal();
            }
            catch (Exception exception)
            {
                try
                {
                    Trace.WriteLine("Failed to dispose COM instance from finalizer because: " + exception);
                }
                catch
                {
                }
            }
        }
    }
}
