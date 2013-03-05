//-----------------------------------------------------------------------
// <copyright file="CallbackWrappersTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the code that wraps a JET_CALLBACK.
    /// </summary>
    [TestClass]
    public class CallbackWrappersTests
    {
        /// <summary>
        /// Make sure the CreateCallback call returns a different
        /// callback each time.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the CreateCallback call returns a different callback each time")]
        public void VerifyCreateCallbackReturnsUniqueCallbacks()
        {
            Assert.AreNotSame(CreateCallback(), CreateCallback());
        }

        /// <summary>
        /// Verify that the the CallbackWrapper calls the wrapped callback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the the CallbackWrapper calls the wrapped callback")]
        public void VerifyWrapperCallsCallback()
        {
            bool callbackWasCalled = false;
            var wrapper = new JetCallbackWrapper(
                (sesid, dbid, tableid, cbtyp, arg1, arg2, context, unused) =>
                {
                    callbackWasCalled = true;
                    return JET_err.Success;
                });

            wrapper.NativeCallback(IntPtr.Zero, 0, IntPtr.Zero, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            Assert.IsTrue(callbackWasCalled);
        }

        /// <summary>
        /// Verify that IsWrapping returns true for a match with the wrapped callback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that IsWrapping returns true for a match with the wrapped callback")]
        public void VerifyIsWrappingReturnsTrueForMatch()
        {
            JET_CALLBACK callback = CreateCallback();
            var wrapper = new JetCallbackWrapper(callback);
            Assert.IsTrue(wrapper.IsWrapping(callback));
            GC.KeepAlive(callback);
        }

        /// <summary>
        /// Verify that IsWrapping returns false when there is no match with the wrapped callback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that IsWrapping returns false when there is no match with the wrapped callback")]
        public void VerifyIsWrappingReturnsFalseForNoMatch()
        {
            JET_CALLBACK callback = CreateCallback();
            var wrapper = new JetCallbackWrapper(callback);
            Assert.IsFalse(wrapper.IsWrapping(CreateCallback()));
            GC.KeepAlive(callback);
        }

        /// <summary>
        /// Verify that the the CallbackWrapper returns the value from the wrapped callback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the the CallbackWrapper returns the value from the wrapped calblack")]
        public void VerifyWrapperReturnsReturnCode()
        {
            var wrapper = new JetCallbackWrapper(
                (sesid, dbid, tableid, cbtyp, arg1, arg2, context, unused) => JET_err.WriteConflict);

            Assert.AreEqual(
                JET_err.WriteConflict,
                wrapper.NativeCallback(IntPtr.Zero, 0, IntPtr.Zero, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero));
        }

        /// <summary>
        /// Verify that the the CallbackWrapper passes the expected arguments.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the the CallbackWrapper passes its arguments to the wrapped callback")]
        public void VerifyWrapperPassesArguments()
        {
            JET_SESID expectedSesid = new JET_SESID { Value = (IntPtr)1 };
            JET_DBID expectedDbid = new JET_DBID { Value = 2 };
            JET_TABLEID expectedTableid = new JET_TABLEID { Value = (IntPtr)3 };
            JET_cbtyp expectedCbtyp = JET_cbtyp.AfterReplace;
            object expectedArg1 = null;
            object expectedArg2 = null;
            IntPtr expectedContext = (IntPtr)4;

            JET_SESID actualSesid = new JET_SESID();
            JET_DBID actualDbid = new JET_DBID();
            JET_TABLEID actualTableid = new JET_TABLEID();
            JET_cbtyp actualCbtyp = JET_cbtyp.Null;
            object actualArg1 = null;
            object actualArg2 = null;
            IntPtr actualContext = new IntPtr();

            var wrapper = new JetCallbackWrapper(
                (sesid, dbid, tableid, cbtyp, arg1, arg2, context, unused) =>
                {
                    actualSesid = sesid;
                    actualDbid = dbid;
                    actualTableid = tableid;
                    actualCbtyp = cbtyp;
                    actualArg1 = arg1;
                    actualArg2 = arg2;
                    actualContext = context;
                    return JET_err.Success;
                });

            wrapper.NativeCallback(
                expectedSesid.Value,
                expectedDbid.Value,
                expectedTableid.Value,
                (uint)expectedCbtyp,
                IntPtr.Zero,
                IntPtr.Zero,
                expectedContext,
                IntPtr.Zero);

            Assert.AreEqual(expectedSesid, actualSesid);
            Assert.AreEqual(expectedDbid, actualDbid);
            Assert.AreEqual(expectedTableid, actualTableid);
            Assert.AreEqual(expectedCbtyp, actualCbtyp);
            Assert.AreEqual(expectedArg1, actualArg1);
            Assert.AreEqual(expectedArg2, actualArg2);
            Assert.AreEqual(expectedContext, actualContext);
        }

        /// <summary>
        /// Verify that the the CallbackWrapper catches exceptions from the callback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the the CallbackWrapper catches exceptions from the callback")]
        public void VerifyExceptionInCallbackIsCaught()
        {
            var wrapper = new JetCallbackWrapper(
                (sesid, dbid, tableid, cbtyp, arg1, arg2, context, unused) =>
                {
                    throw new ArgumentNullException();
                });

            Assert.AreEqual(
                JET_err.CallbackFailed,
                wrapper.NativeCallback(IntPtr.Zero, 0, IntPtr.Zero, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero));
        }

        /// <summary>
        /// Verify that adding an callback returns a wrapper.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding an callback returns a wrapper")]
        public void VerifyAddingCallbackReturnsWrapper()
        {
            JET_CALLBACK callback = CreateCallback();

            var callbackWrappers = new CallbackWrappers();
            var wrapper = callbackWrappers.Add(callback);

            Assert.IsNotNull(callbackWrappers.Add(callback));
        }

        /// <summary>
        /// Verify that adding an existing callback returns the same wrapper.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding an existing callback returns the same wrapper")]
        public void VerifyAddingSameCallbackReturnsSameWrapper()
        {
            JET_CALLBACK callback = CreateCallback();

            var callbackWrappers = new CallbackWrappers();
            var wrapper = callbackWrappers.Add(callback);

            Assert.AreEqual(wrapper, callbackWrappers.Add(callback));
        }

        /// <summary>
        /// Make sure the wrapper isn't removed when the callback is alive.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the the CallbackWrapper is alive when the callback is alive")]
        public void VerifyWrapperIsAliveWhenCallbackIsAlive()
        {
            JET_CALLBACK callback = CreateCallback();

            var callbackWrappers = new CallbackWrappers();
            var wrapperRef = new WeakReference(callbackWrappers.Add(callback));

            RunFullGarbageCollection();
            callbackWrappers.Collect();
            RunFullGarbageCollection();

            Assert.IsTrue(wrapperRef.IsAlive);

            // Avoid premature collection of these objects
            GC.KeepAlive(callback);
            GC.KeepAlive(callbackWrappers);
        }

        /// <summary>
        /// Make sure the wrapper is removed once the callback is collected.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the wrapper is removed once the callback is collected")]
        public void VerifyWrapperIsCollectedWhenCallbackIsCollected()
        {
            JET_CALLBACK callback = CreateCallback();
            var callbackRef = new WeakReference(callback);

            var callbackWrappers = new CallbackWrappers();
            var wrapperRef = new WeakReference(callbackWrappers.Add(callback));

            callback = null;

            RunFullGarbageCollection();
            callbackWrappers.Collect();
            RunFullGarbageCollection();

            Assert.IsFalse(callbackRef.IsAlive);
            Assert.IsFalse(wrapperRef.IsAlive);

            // Avoid premature collection of this objects
            GC.KeepAlive(callbackWrappers);
        }

        /// <summary>
        /// Make sure the CallbackWrappers class removes unused entries (avoiding memory leaks).
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Verify the CallbackWrappers class removes unused entries (avoiding memory leaks)")]
        public void VerifyCallbackWrappersCollectsUnusedWrappers()
        {
            DateTime endTime = DateTime.Now + TimeSpan.FromSeconds(19);

            var callbackWrappers = new CallbackWrappers();

            RunFullGarbageCollection();
            long memoryAtStart = GC.GetTotalMemory(true);

            while (DateTime.Now < endTime)
            {
                for (int i = 0; i < 128; ++i)
                {
                    CreateCallbackWrapper(callbackWrappers);
                }

                RunFullGarbageCollection();
                callbackWrappers.Collect();
                RunFullGarbageCollection();
            }

            RunFullGarbageCollection();
            long memoryAtEnd = GC.GetTotalMemory(true);
            GC.KeepAlive(callbackWrappers);

            long memory = memoryAtEnd - memoryAtStart;
            Console.WriteLine("{0:N0} bytes used", memory);
            Assert.IsTrue(memory < 1024 * 1024, "Test used too much memory. JetCallbackWrapper objects weren't collected.");
        }

        /// <summary>
        /// Verify that the NativeCallback isn't garbage collected.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the NativeCallback isn't garbage collected")]
        public void VerifyNativeCallbackIsNotGarbageCollected()
        {
            JET_CALLBACK callback = CreateCallback();

            var callbackWrappers = new CallbackWrappers();
            JetCallbackWrapper wrapper = callbackWrappers.Add(callback);
            WeakReference weakRef = new WeakReference(wrapper.NativeCallback);
            RunFullGarbageCollection();
            Assert.IsTrue(weakRef.IsAlive);
            GC.KeepAlive(wrapper);
        }

        /// <summary>
        /// Run a full garbage collection.
        /// </summary>
        private static void RunFullGarbageCollection()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        /// <summary>
        /// Create a new wrapped callback.
        /// </summary>
        /// <param name="callbackWrappers">The CallbackWrappers to add the callback to.</param>
        private static void CreateCallbackWrapper(CallbackWrappers callbackWrappers)
        {
            callbackWrappers.Add(CreateCallback());
        }

        /// <summary>
        /// Allocate a new JET_CALLBACK. We want each callback to be unique.
        /// To force a new callback to be created we use a closure.
        /// </summary>
        /// <returns>A new JET_CALLBACK</returns>
        private static JET_CALLBACK CreateCallback()
        {
            var rand = new Random();
            int err = rand.Next();
            return (sesid, dbid, tableid, cbtyp, arg1, arg2, context, unused) => (JET_err)err;
        }
    }
}