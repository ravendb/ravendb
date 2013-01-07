//-----------------------------------------------------------------------
// <copyright file="InstanceTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Rhino.Mocks;
    using Rhino.Mocks.Constraints;

    /// <summary>
    /// Test the disposable Instance class, which wraps a JET_INSTANCE.
    /// </summary>
    [TestClass]
    public class InstanceTests
    {
        #region Setup/Teardown

        /// <summary>
        /// Verifies no instances are leaked.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        #endregion

        /// <summary>
        /// Allocate an instance, but don't initialize it.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Create an Instance without initializing it")]
        public void CreateInstanceNoInit()
        {
            string dir = SetupHelper.CreateRandomDirectory();
            using (var instance = new Instance("createnoinit"))
            {
                Assert.AreNotEqual(JET_INSTANCE.Nil, instance.JetInstance);
                Assert.IsNotNull(instance.Parameters);

                instance.Parameters.LogFileDirectory = dir;
                instance.Parameters.SystemDirectory = dir;
                instance.Parameters.TempDirectory = dir;
            }

            Cleanup.DeleteDirectoryWithRetry(dir);
        }

        /// <summary>
        /// Test implicit conversion to a JET_INSTANCE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test implicit conversation of an Instance to a JET_INSTANCE")]
        public void InstanceCanConvertToJetInstance()
        {
            using (var instance = new Instance("converttoinstance"))
            {
                JET_INSTANCE jetinstance = instance;
                Assert.AreEqual(jetinstance, instance.JetInstance);
            }
        }

        /// <summary>
        /// When the instance is closed JetTerm should be called.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that JetTerm is called when the instance is closed")]
        public void VerifyInstanceCallsJetTerm()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                var jetInstance = new JET_INSTANCE { Value = (IntPtr)0x1 };

                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(jetInstance).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int)JET_err.Success);
                Expect.Call(
                    mockApi.JetInit2(
                        ref Arg<JET_INSTANCE>.Ref(Is.Equal(jetInstance), jetInstance).Dummy,
                        Arg<InitGrbit>.Is.Anything))
                    .Return((int)JET_err.Success);
                Expect.Call(mockApi.JetTerm(Arg<JET_INSTANCE>.Is.Equal(jetInstance))).Return((int)JET_err.Success);
                mocks.ReplayAll();

                using (var instance = new Instance("testterm"))
                {
                    instance.Init();
                    instance.Close();
                }

                mocks.VerifyAll();
            }
        }

        /// <summary>
        /// When JetCreateInstance2 fails the instance isn't initialized
        /// so it shouldn't be freed.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test cleanup when JetCreateInstance2 fails")]
        public void VerifyInstanceDoesNotCallJetTermWhenCreateInstanceFails()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(JET_INSTANCE.Nil).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int)JET_err.InvalidName);
                mocks.ReplayAll();

                try
                {
                    using (var instance = new Instance("testfail"))
                    {
                        Assert.Fail("Expected an EsentErrorException");
                    }
                }
                catch (EsentErrorException)
                {
                    // expected
                }

                mocks.VerifyAll();
            }
        }

        /// <summary>
        /// When JetInit fails the instance isn't initialized
        /// so it shouldn't be terminated.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify JetTerm is not called when JetInit2 fails")]
        public void VerifyInstanceDoesNotCallJetTermWhenJetInitFails()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                var jetInstance = new JET_INSTANCE { Value = (IntPtr)0x1 };

                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(jetInstance).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int)JET_err.Success);
                Expect.Call(
                    mockApi.JetInit2(
                        ref Arg<JET_INSTANCE>.Ref(Is.Equal(jetInstance), JET_INSTANCE.Nil).Dummy,
                        Arg<InitGrbit>.Is.Anything))
                    .Return((int)JET_err.OutOfMemory);
                mocks.ReplayAll();

                try
                {
                    using (var instance = new Instance("testfail2"))
                    {
                        instance.Init();
                        Assert.Fail("Expected an EsentErrorException");
                    }
                }
                catch (EsentErrorException)
                {
                    // expected
                }

                mocks.VerifyAll();
            }
        }

        /// <summary>
        /// When JetInit fails the instance isn't initialized
        /// so we shouldn't be able to use it.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify JetTerm is not called when JetInit2 fails")]
        public void VerifyInitThrowsExceptionAfterJetInitFails()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                var jetInstance = new JET_INSTANCE { Value = (IntPtr)0x1 };

                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(jetInstance).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int)JET_err.Success);
                Expect.Call(
                    mockApi.JetInit2(
                        ref Arg<JET_INSTANCE>.Ref(Is.Equal(jetInstance), JET_INSTANCE.Nil).Dummy,
                        Arg<InitGrbit>.Is.Anything))
                    .Return((int)JET_err.OutOfMemory);
                mocks.ReplayAll();

                Instance instance = new Instance("testfail3");
                try
                {
                    instance.Init();
                    Assert.Fail("Expected an EsentErrorException");
                }
                catch (EsentErrorException)
                {
                    // expected
                }

                mocks.VerifyAll();

                try
                {
                    instance.Init();
                    Assert.Fail("Expected an ObjectDisposedException");
                }
                catch (ObjectDisposedException)
                {                    
                    // Expected
                }
            }
        }

        /// <summary>
        /// When JetInit3 fails the instance isn't initialized
        /// so it shouldn't be terminated.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify JetTerm is not called when JetInit3 fails")]
        public void VerifyInstanceDoesNotCallJetTermWhenJetInit3Fails()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                var jetInstance = new JET_INSTANCE { Value = (IntPtr)0x1 };

                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(jetInstance).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int)JET_err.Success);
                Expect.Call(
                    mockApi.JetInit3(
                        ref Arg<JET_INSTANCE>.Ref(Is.Equal(jetInstance), JET_INSTANCE.Nil).Dummy,
                        Arg<JET_RSTINFO>.Is.Anything,
                        Arg<InitGrbit>.Is.Anything))
                    .Return((int)JET_err.OutOfMemory);
                mocks.ReplayAll();

                try
                {
                    using (var instance = new Instance("testfail4"))
                    {
                        instance.Init(null, InitGrbit.None);
                        Assert.Fail("Expected an EsentErrorException");
                    }
                }
                catch (EsentErrorException)
                {
                    // expected
                }

                mocks.VerifyAll();
            }
        }

        /// <summary>
        /// When JetInit3 fails the instance isn't initialized
        /// so we shouldn't be able to use it.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify JetTerm is not called when JetInit3 fails")]
        public void VerifyInitThrowsExceptionAfterJetInit3Fails()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                var jetInstance = new JET_INSTANCE { Value = (IntPtr)0x1 };

                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(jetInstance).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int)JET_err.Success);
                Expect.Call(
                    mockApi.JetInit3(
                        ref Arg<JET_INSTANCE>.Ref(Is.Equal(jetInstance), JET_INSTANCE.Nil).Dummy,
                        Arg<JET_RSTINFO>.Is.Anything,
                        Arg<InitGrbit>.Is.Anything))
                    .Return((int)JET_err.OutOfMemory);
                mocks.ReplayAll();

                Instance instance = new Instance("testfail5");
                try
                {
                    instance.Init(null, InitGrbit.None);
                    Assert.Fail("Expected an EsentErrorException");
                }
                catch (EsentErrorException)
                {
                    // expected
                }

                mocks.VerifyAll();

                try
                {
                    instance.Init();
                    Assert.Fail("Expected an ObjectDisposedException");
                }
                catch (ObjectDisposedException)
                {
                    // Expected
                }
            }
        }

        /// <summary>
        /// Allocate an instance and initialize it.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create and initialize an instance")]
        public void CreateInstanceInit()
        {
            string dir = SetupHelper.CreateRandomDirectory();
            using (var instance = new Instance("createinit"))
            {
                instance.Parameters.LogFileDirectory = dir;
                instance.Parameters.SystemDirectory = dir;
                instance.Parameters.TempDirectory = dir;
                instance.Parameters.LogFileSize = 512; // 512Kb
                instance.Parameters.NoInformationEvent = true;
                instance.Init();
            }

            Cleanup.DeleteDirectoryWithRetry(dir);
        }

        /// <summary>
        /// Allocate an instance with a display name.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Create and initialize an instance with a display name")]
        public void CreateInstanceWithDisplayName()
        {
            using (var instance = new Instance(Guid.NewGuid().ToString(), "Friendly Display Name"))
            {
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Parameters.Recovery = false;
                instance.Init();
            }
        }

        /// <summary>
        /// Allocate an instance and initialize it and then terminate.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create, intialize and terminate an instance")]
        public void CreateInstanceInitTerm()
        {
            string dir = SetupHelper.CreateRandomDirectory();
            using (var instance = new Instance("initterm"))
            {
                instance.Parameters.LogFileDirectory = dir;
                instance.Parameters.SystemDirectory = dir;
                instance.Parameters.TempDirectory = dir;
                instance.Parameters.LogFileSize = 256; // 256Kb
                instance.Parameters.NoInformationEvent = true;
                instance.Init();
                instance.Term();
                Cleanup.DeleteDirectoryWithRetry(dir);    // only works if the instance is terminated
            }
        }

        /// <summary>
        /// Initialize an instance with recovery options.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Initialize an instance with recovery options")]
        public void InitWithRecoveryOptions()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            using (var instance = new Instance("initwithrecoveryoptions"))
            {
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Parameters.Recovery = false;
                instance.Parameters.NoInformationEvent = true;
                instance.Init(null, InitGrbit.None);
            }
        }

        /// <summary>
        /// Make sure that garbage collection can close an instance
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that garbage collection terminates instances")]
        public void VerifyInstanceCanBeFinalized()
        {
            for (int i = 0; i < 3; ++i)
            {
                // If finalization doesn't close the instance then subseqent 
                // creation attempts will fail
                CreateOneInstance();

                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        /// <summary>
        /// Make sure that accessing the instance of a closed object throws an
        /// exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that using the JET_INSTANCE of a terminated Instance throws an exception")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void JetInstanceThrowsExceptionWhenInstanceIsClosed()
        {
            var instance = new Instance("closed");
            SetupHelper.SetLightweightConfiguration(instance);
            instance.Init();
            instance.Term();
            JET_INSTANCE x = instance.JetInstance;
        }

        /// <summary>
        /// Make sure that accessing the instance of a disposed object throws an
        /// exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that using the JET_INSTANCE of a disposed Instance throws an exception")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void JetInstanceThrowsExceptionWhenInstanceIsDisposed()
        {
            var instance = new Instance("disposed");
            instance.Dispose();
            JET_INSTANCE x = instance.JetInstance;
        }

        /// <summary>
        /// Make sure that accessing the parameters of a disposed object throws an
        /// exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that using the parameters of a disposed Instance throws an exception")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void ParametersThrowsExceptionWhenInstanceIsDisposed()
        {
            var instance = new Instance("disposed2");
            instance.Dispose();
            InstanceParameters x = instance.Parameters;
        }

        /// <summary>
        /// Make sure that calling Init on a disposed object throws an
        /// exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that calling Init on a disposed Instance throws an exception")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void InitThrowsExceptionWhenInstanceIsDisposed()
        {
            var instance = new Instance("disposed3");
            instance.Dispose();
            instance.Init();
        }

        /// <summary>
        /// Make sure that calling Init3 on a disposed object throws an
        /// exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that calling Init3 on a disposed Instance throws an exception")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void Init3ThrowsExceptionWhenInstanceIsDisposed()
        {
            var instance = new Instance("disposed3");
            instance.Dispose();
            instance.Init(null, InitGrbit.None);
        }

        /// <summary>
        /// Make sure that calling Term on a disposed object throws an
        /// exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that calling Term on a disposed Instance throws an exception")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TermThrowsExceptionWhenInstanceIsDisposed()
        {
            var instance = new Instance("disposed4");
            instance.Dispose();
            instance.Term();
        }

        /// <summary>
        /// Verify AppDomain unload terminates the Instance.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify AppDomain unload terminates the Instance")]
        public void VerifyAppDomainUnloadTerminatesInstance()
        {
            // By default vstesthost sets the AppDomain root directory
            // to the vstesthost.exe path. We need to change that so
            // our assembly can be found.
            var setup = new AppDomainSetup
            {
                ApplicationBase = Environment.CurrentDirectory
            };

            var otherDomain = AppDomain.CreateDomain("InstanceTest", null, setup);
            CreateInAppDomain<InstanceWrapper>(otherDomain);
            AppDomain.Unload(otherDomain);

            // If unloading the AppDomain didn't terminate the instance
            // this will fail with an InstanceNameInUse error.
            var instanceWrapper = new InstanceWrapper();
            instanceWrapper.Cleanup();
        }

        /// <summary>
        /// Test thread aborts during init and term.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test thread aborts during Instance Init/Term")]
        public void TestThreadAbortDuringInstanceInitTerm()
        {
            var rand = new Random();
            var timeToRun = TimeSpan.FromSeconds(19);
            var startTime = DateTime.UtcNow;

            const string InstanceNameTemplate = "ThreadAbortTest{0}";
            int numThreads = 1; // Should be Environment.ProcessorCount (see assert at end)

            int iteration = 0;

            // Supressing execution context flow speeds up thread creation.
            ExecutionContext.SuppressFlow();

            while (DateTime.UtcNow < (startTime + timeToRun))
            {
                var threads = new Thread[numThreads];

                for (int i = 0; i < numThreads; ++i)
                {
                    string instanceName = String.Format(InstanceNameTemplate, i);
                    threads[i] = new Thread(() => InstanceInitTermThread(instanceName));
                    threads[i].Start();
                }

                Thread.Sleep(TimeSpan.FromMilliseconds(rand.Next(0, 200)));

                foreach (Thread thread in threads)
                {
                    thread.Abort();
                }

                foreach (Thread thread in threads)
                {
                    thread.Join();
                }

                iteration++;
            }

            ExecutionContext.RestoreFlow();

            // Make sure the instance name is still available
            for (int i = 0; i < numThreads; ++i)
            {
                string instanceName = String.Format(InstanceNameTemplate, i);
                using (new Instance(instanceName))
                {
                }
            }

            Console.WriteLine("{0} iterations", iteration);

            if (Environment.ProcessorCount > 1)
            {
                Assert.Inconclusive("ESENT crashes during Init/Term prevent this test from running reliably with multiple threads");
            }
        }

        /// <summary>
        /// Create an instance of the specified type in the given AppDomain.
        /// </summary>
        /// <typeparam name="T">
        /// The type to create an instance of.
        /// </typeparam>
        /// <param name="appDomain">
        /// The AppDomain to create the instance in.
        /// </param>
        /// <returns>
        /// A proxy for an instance of the type, created in the given AppDomain.
        /// </returns>
        private static T CreateInAppDomain<T>(AppDomain appDomain)
        {
            string assembly = typeof(T).Assembly.FullName;
            string type = typeof(T).FullName;
            return (T)appDomain.CreateInstanceAndUnwrap(assembly, type);   
        }

        /// <summary>
        /// Init and term an instance. This is used to make sure the instance
        /// is always cleaned up when the thread is terminated. If the cleanup
        /// is missed the next instance create will fail.
        /// </summary>
        /// <param name="instanceName">
        /// The name of the instance to create.
        /// </param>
        private static void InstanceInitTermThread(string instanceName)
        {
            try
            {
                while (true)
                {
                    using (var instance = new Instance(instanceName))
                    {
                        SetupHelper.SetLightweightConfiguration(instance);
                        instance.Init();
                        instance.Term();
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // Actually letting the thread abort will fail the test, exit
                // gracefully instead.
                Thread.ResetAbort();
            }
            catch (EsentErrorException ex)
            {
                Console.WriteLine("Got exception {0}", ex);
                Assert.Fail("Got exception {0}", ex);
            }
        }

        /// <summary>
        /// Create an instance and abandon it. Garbage collection should
        /// be able to finalize the instance.
        /// </summary>
        private static void CreateOneInstance()
        {
            var instance = new Instance("finalize_me");
            SetupHelper.SetLightweightConfiguration(instance);
            instance.Init();
        }

        /// <summary>
        /// A wrapper object used to initialize an instance in another appdomain.
        /// </summary>
        private class InstanceWrapper : MarshalByRefObject
        {
            /// <summary>
            /// The instance.
            /// </summary>
            private readonly Instance instance;

            /// <summary>
            /// Initializes a new instance of the <see cref="InstanceWrapper"/> class.
            /// </summary>
            public InstanceWrapper()
            {
                this.instance = new Instance("InstanceWrapper");
                SetupHelper.SetLightweightConfiguration(this.instance);
            }

            /// <summary>
            /// Cleans up resources.
            /// </summary>
            public void Cleanup()
            {
                using (this.instance)
                {
                }
            }
        }
    }
}