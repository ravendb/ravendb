//-----------------------------------------------------------------------
// <copyright file="JetCallbackWrapper.cs" company="Microsoft Corporation">
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

    /// <summary>
    /// A multi-purpose callback function used by the database engine to inform
    /// the application of an event involving online defragmentation and cursor
    /// state notifications. 
    /// </summary>
    /// <param name="sesid">The session for which the callback is being made.</param>
    /// <param name="dbid">The database for which the callback is being made.</param>
    /// <param name="tableid">The cursor for which the callback is being made.</param>
    /// <param name="cbtyp">The operation for which the callback is being made.</param>
    /// <param name="arg1">First callback-specific argument.</param>
    /// <param name="arg2">Second callback-specific argument.</param>
    /// <param name="context">Callback context.</param>
    /// <param name="unused">This parameter is not used.</param>
    /// <returns>An ESENT error code.</returns>
    internal delegate JET_err NATIVE_CALLBACK(
        IntPtr sesid,
        uint dbid,
        IntPtr tableid,
        uint cbtyp,
        IntPtr arg1,
        IntPtr arg2,
        IntPtr context,
        IntPtr unused);

    /// <summary>
    /// Wraps a NATIVE_CALLBACK callback around a JET_CALLBACK. This is
    /// used to catch exceptions and provide argument conversion.
    /// </summary>
    internal sealed class JetCallbackWrapper
    {        
        /// <summary>
        /// API call tracing.
        /// </summary>
        private static readonly TraceSwitch traceSwitch = new TraceSwitch("ESENT JetCallbackWrapper", "Wrapper around unmanaged ESENT callback");

        /// <summary>
        /// The wrapped status callback.
        /// </summary>
        private readonly WeakReference wrappedCallback;

        /// <summary>
        /// The native version of the callback. This will actually be a closure
        /// because we are calling a non-static method. Keep track of it here
        /// to make sure that it isn't garbage collected.
        /// </summary>
        private readonly NATIVE_CALLBACK nativeCallback;

        /// <summary>
        /// Initializes static members of the <see cref="JetCallbackWrapper"/> class. 
        /// </summary>
        static JetCallbackWrapper()
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
        /// Initializes a new instance of the JetCallbackWrapper class.
        /// </summary>
        /// <param name="callback">
        /// The managed callback to use.
        /// </param>
        public JetCallbackWrapper(JET_CALLBACK callback)
        {
            this.wrappedCallback = new WeakReference(callback);
            this.nativeCallback = this.CallbackImpl;
            Debug.Assert(this.wrappedCallback.IsAlive, "Callback isn't alive");
        }

        /// <summary>
        /// Gets a value indicating whether the wrapped callback has been garbage
        /// collected.
        /// </summary>
        public bool IsAlive
        {
            get
            {
                return this.wrappedCallback.IsAlive;
            }
        }

        /// <summary>
        /// Gets a NATIVE_CALLBACK callback that wraps the managed callback.
        /// </summary>
        public NATIVE_CALLBACK NativeCallback
        {
            get
            {
                return this.nativeCallback;
            }
        }

        /// <summary>
        /// Determine if the callback is wrapping the specified JET_CALLBACK.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <returns>True if this wrapper is wrapping the callback.</returns>
        public bool IsWrapping(JET_CALLBACK callback)
        {
            return callback.Equals(this.wrappedCallback.Target);
        }

        /// <summary>
        /// Callback function for native code. We don't want to throw an exception through
        /// unmanaged ESENT because that will corrupt ESENT's internal state. Instead we
        /// catch all exceptions and return an error instead. We use a CER to make catching
        /// the exceptions as reliable as possible.
        /// </summary>
        /// <param name="nativeSesid">The session for which the callback is being made.</param>
        /// <param name="nativeDbid">The database for which the callback is being made.</param>
        /// <param name="nativeTableid">The cursor for which the callback is being made.</param>
        /// <param name="nativeCbtyp">The operation for which the callback is being made.</param>
        /// <param name="arg1">First callback-specific argument.</param>
        /// <param name="arg2">Second callback-specific argument.</param>
        /// <param name="nativeContext">Callback context.</param>
        /// <param name="unused">This parameter is not used.</param>
        /// <returns>An ESENT error code.</returns>
        private JET_err CallbackImpl(
            IntPtr nativeSesid,
            uint nativeDbid,
            IntPtr nativeTableid,
            uint nativeCbtyp,
            IntPtr arg1,
            IntPtr arg2,
            IntPtr nativeContext,
            IntPtr unused)
        {
            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                var sesid = new JET_SESID { Value = nativeSesid };
                var dbid = new JET_DBID { Value = nativeDbid };
                var tableid = new JET_TABLEID { Value = nativeTableid };
                JET_cbtyp cbtyp = (JET_cbtyp)nativeCbtyp;

                Debug.Assert(this.wrappedCallback.IsAlive, "Wrapped callback has been garbage collected");

                // This will throw an exception if the wrapped callback has been collected. The exception
                // will be handled below.
                JET_CALLBACK callback = (JET_CALLBACK)this.wrappedCallback.Target;
                return callback(sesid, dbid, tableid, cbtyp, null, null, nativeContext, IntPtr.Zero);
            }
            catch (Exception ex)
            {                
                // Thread aborts aren't handled here. ESENT callbacks can execute on client threads or
                // internal ESENT threads so it isn't clear what should be done on an abort.
                Trace.WriteLineIf(
                    traceSwitch.TraceWarning,
                    String.Format(CultureInfo.InvariantCulture, "Caught Exception {0}", ex));
                return JET_err.CallbackFailed;
            }
        }
    }
}
