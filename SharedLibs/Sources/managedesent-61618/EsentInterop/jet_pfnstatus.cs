//-----------------------------------------------------------------------
// <copyright file="jet_pfnstatus.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;

    /// <summary>
    /// Receives information about the progress of long-running operations,
    /// such as defragmentation, backup, or restore operations. During such
    /// operations, the database engine calls this callback function to give
    ///  an update on the progress of the operation.
    /// </summary>
    /// <param name="sesid">
    /// The session with which the long running operation was called.
    /// </param>
    /// <param name="snp">The type of operation.</param>
    /// <param name="snt">The status of the operation.</param>
    /// <param name="data">Optional data. May be a <see cref="JET_SNPROG"/>.</param>
    /// <returns>An error code.</returns>
    public delegate JET_err JET_PFNSTATUS(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data);

    /// <summary>
    /// Receives information about the progress of long-running operations,
    /// such as defragmentation, backup, or restore operations. During such
    /// operations, the database engine calls this callback function to give
    ///  an update on the progress of the operation.
    /// </summary>
    /// <remarks>
    /// This is the internal version of the callback. The final parameter is
    /// a void* pointer, which may point to a NATIVE_SNPROG.
    /// </remarks>
    /// <param name="nativeSesid">
    /// The session with which the long running operation was called.
    /// </param>
    /// <param name="snp">The type of operation.</param>
    /// <param name="snt">The status of the operation.</param>
    /// <param name="snprog">Optional <see cref="NATIVE_SNPROG"/>.</param>
    /// <returns>An error code.</returns>
    internal delegate JET_err NATIVE_PFNSTATUS(IntPtr nativeSesid, uint snp, uint snt, IntPtr snprog);

    /// <summary>
    /// Wraps a NATIVE_PFNSTATUS callback around a JET_PFNSTATUS. This is
    /// used to convert the snprog argument to a managed snprog.
    /// </summary>
    internal sealed class StatusCallbackWrapper
    {
        /// <summary>
        /// API call tracing.
        /// </summary>
        private static readonly TraceSwitch TraceSwitch = new TraceSwitch("ESENT StatusCallbackWrapper", "Wrapper around unmanaged ESENT status callback");

        /// <summary>
        /// The wrapped status callback.
        /// </summary>
        private readonly JET_PFNSTATUS wrappedCallback;

        /// <summary>
        /// The native version of the callback. This will be a closure (because we are wrapping
        /// a non-static method) so keep track of it here to make sure it isn't garbage collected.
        /// </summary>
        private readonly NATIVE_PFNSTATUS nativeCallback;

        /// <summary>
        /// Initializes static members of the <see cref="StatusCallbackWrapper"/> class. 
        /// </summary>
        static StatusCallbackWrapper()
        {
            // We don't want a JIT failure when trying to execute the callback
            // because that would throw an exception through ESENT, corrupting it.
            // It is fine for the wrapped callback to fail because CallbackImpl
            // will catch the exception and deal with it.
            RuntimeHelpers.PrepareMethod(typeof(StatusCallbackWrapper).GetMethod(
                "CallbackImpl",
                BindingFlags.NonPublic | BindingFlags.Instance).MethodHandle);    
        }

        /// <summary>
        /// Initializes a new instance of the StatusCallbackWrapper class.
        /// </summary>
        /// <param name="wrappedCallback">
        /// The managed callback to use.
        /// </param>
        public StatusCallbackWrapper(JET_PFNSTATUS wrappedCallback)
        {
            this.wrappedCallback = wrappedCallback;
            this.nativeCallback = this.CallbackImpl;
        }

        /// <summary>
        /// Gets a NATIVE_PFNSTATUS callback that wraps the managed callback.
        /// </summary>
        public NATIVE_PFNSTATUS NativeCallback
        {
            get
            {
                return this.nativeCallback;
            }
        }

        /// <summary>
        /// Gets or sets the saved exception. If the callback throws an exception
        /// it is saved here and should be rethrown when the API call finishes.
        /// </summary>
        private Exception SavedException { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the thread was aborted during
        /// the callback.
        /// </summary>
        private bool ThreadWasAborted { get; set; }

        /// <summary>
        /// If an exception was generated during a callback throw it.
        /// </summary>
        public void ThrowSavedException()
        {
            if (this.ThreadWasAborted)
            {
                Thread.CurrentThread.Abort();
            }

            if (null != this.SavedException)
            {
                throw this.SavedException;
            }
        }

        /// <summary>
        /// Callback function for native code. We don't want to throw an exception through
        /// unmanaged ESENT because that will corrupt ESENT's internal state. Instead we
        /// catch all exceptions and return an error instead. We use a CER to make catching
        /// the exceptions as reliable as possible.
        /// </summary>
        /// <param name="nativeSesid">
        /// The session with which the long running operation was called.
        /// </param>
        /// <param name="nativeSnp">The type of operation.</param>
        /// <param name="nativeSnt">The status of the operation.</param>
        /// <param name="nativeData">Optional <see cref="NATIVE_SNPROG"/>.</param>
        /// <returns>An error code.</returns>
        private JET_err CallbackImpl(IntPtr nativeSesid, uint nativeSnp, uint nativeSnt, IntPtr nativeData)
        {
            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                var sesid = new JET_SESID { Value = nativeSesid };
                JET_SNP snp = (JET_SNP)nativeSnp;
                JET_SNT snt = (JET_SNT)nativeSnt;
                object data = CallbackDataConverter.GetManagedData(nativeData, snp, snt);
                return this.wrappedCallback(sesid, snp, snt, data);
            }
            catch (ThreadAbortException)
            {
                Trace.WriteLineIf(TraceSwitch.TraceWarning, "Caught ThreadAbortException");

                // Stop the thread abort and let the unmanaged ESENT code finish.
                // ThrowSavedException will call Thread.Abort() again.
                this.ThreadWasAborted = true;
                Thread.ResetAbort();
                return JET_err.CallbackFailed;
            }
            catch (Exception ex)
            {
                Trace.WriteLineIf(
                    TraceSwitch.TraceWarning,
                    String.Format(CultureInfo.InvariantCulture, "Caught Exception {0}", ex));
                this.SavedException = ex;
                return JET_err.CallbackFailed;
            }

            // What happens if the thread is aborted here, outside of the CER?
            // We probably throw the exception through ESENT, which isn't good.
        }
    }
}
