//-----------------------------------------------------------------------
// <copyright file="InstanceTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
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
                    .Return((int) JET_err.InvalidName);
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
                var jetInstance = new JET_INSTANCE { Value = (IntPtr) 0x1 };

                Expect.Call(
                    mockApi.JetCreateInstance2(
                        out Arg<JET_INSTANCE>.Out(jetInstance).Dummy,
                        Arg<string>.Is.Anything,
                        Arg<string>.Is.Anything,
                        Arg<CreateInstanceGrbit>.Is.Anything))
                    .Return((int) JET_err.Success);
                Expect.Call(
                    mockApi.JetInit2(
                        ref Arg<JET_INSTANCE>.Ref(Is.Equal(jetInstance), JET_INSTANCE.Nil).Dummy,
                        Arg<InitGrbit>.Is.Anything))
                    .Return((int) JET_err.OutOfMemory);
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
            int numThreads = Environment.ProcessorCount;

            Assert.Inconclusive("ESENT crashes during Init/Term prevent this test from running reliably");

            int iteration = 0;
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

            // Make sure the instance name is still available
            for (int i = 0; i < numThreads; ++i)
            {
                string instanceName = String.Format(InstanceNameTemplate, i);
                using (new Instance(instanceName))
                {
                }
            }

            Console.WriteLine("{0} iterations", iteration);
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
    }
}
