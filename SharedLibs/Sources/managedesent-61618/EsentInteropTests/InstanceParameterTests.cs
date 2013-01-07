//-----------------------------------------------------------------------
// <copyright file="InstanceParameterTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the InstanceParameters class.
    /// </summary>
    [TestClass]
    public partial class InstanceParameterTests
    {
        /// <summary>
        /// Instance to use.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The InstanceParameters class to use for testing.
        /// </summary>
        private InstanceParameters instanceParameters;

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// </summary>
        [TestInitialize]
        [Description("Setup the InstanceParametersTests fixture")]
        public void Setup()
        {
            Api.JetCreateInstance(out this.instance, Guid.NewGuid().ToString());
            this.instanceParameters = new InstanceParameters(this.instance);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the InstanceParametersTests fixture")]
        public void Teardown()
        {
            Api.JetTerm(this.instance);
        }

        /// <summary>
        /// Verify that the test class has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the test class has setup the test fixture properly")]
        public void VerifyInstanceParametersFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.IsNotNull(this.instanceParameters);
        }

        /// <summary>
        /// Verify that the helper method to return a string system parameter works.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the helper method to return a string system parameter works")]
        public void VerifyGetStringParameterHelperGetsParameter()
        {
            string expected = Any.String;
            JET_param param = JET_param.EventSource;
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, param, 0, expected);
            Assert.AreEqual(expected, this.GetStringParameter(param));
        }

        /// <summary>
        /// Verify that the helper method to return an integer system parameter works.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the helper method to return an integer system parameter works")]
        public void VerifyGetIntegerParameterHelperGetsParameter()
        {
            const int Expected = 100;
            JET_param param = JET_param.MaxOpenTables;
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, param, Expected, string.Empty);
            Assert.AreEqual(Expected, this.GetIntegerParameter(param));
        }

        /// <summary>
        /// Test the BaseName property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the BaseName property")]
        public void SetAndRetrieveInstanceParametersBaseName()
        {
            string expected = "abc";
            this.instanceParameters.BaseName = expected;
            Assert.AreEqual(expected, this.instanceParameters.BaseName);            
        }

        /// <summary>
        /// Test setting the BaseName property sets the parameter on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the BaseName property sets the parameter on the instance")]
        public void VerifySetInstanceParametersBaseName()
        {
            string expected = "xyz";
            this.instanceParameters.BaseName = expected;
            Assert.AreEqual(expected, this.GetStringParameter(JET_param.BaseName));
        }

        /// <summary>
        /// Test the event source property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the event source property")]
        public void SetAndRetrieveInstanceParametersEventSource()
        {
            string expected = Any.String;
            this.instanceParameters.EventSource = expected;
            Assert.AreEqual(expected, this.instanceParameters.EventSource);
        }

        /// <summary>
        /// Test setting the EventSource property sets the parameter on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the EventSource property sets the parameter on the instance")]
        public void VerifySetInstanceParametersEventSource()
        {
            string expected = Any.String;
            this.instanceParameters.EventSource = expected;
            Assert.AreEqual(expected, this.GetStringParameter(JET_param.EventSource));
        }

        /// <summary>
        /// Test the event source key property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the event source key property")]
        public void SetAndRetrieveInstanceParametersEventSourceKey()
        {
            string expected = Any.String;
            this.instanceParameters.EventSourceKey = expected;
            Assert.AreEqual(expected, this.instanceParameters.EventSourceKey);
        }

        /// <summary>
        /// Test setting the EventSourceKey property sets the parameter on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the EventSourceKey property sets the parameter on the instance")]
        public void VerifySetInstanceParametersEventSourceKey()
        {
            string expected = Any.String;
            this.instanceParameters.EventSourceKey = expected;
            Assert.AreEqual(expected, this.GetStringParameter(JET_param.EventSourceKey));
        }

        /// <summary>
        /// Test the MaxSessions property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the MaxSessions property")]
        public void SetAndRetrieveInstanceParametersMaxSessions()
        {
            int expected = 11;
            this.instanceParameters.MaxSessions = expected;
            Assert.AreEqual(expected, this.instanceParameters.MaxSessions);
        }

        /// <summary>
        /// Setting the MaxSessions property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the MaxSessions property should set the parameter on the instance")]
        public void VerifySetInstanceParametersMaxSessions()
        {
            int expected = 40;
            this.instanceParameters.MaxSessions = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.MaxSessions));
        }

        /// <summary>
        /// Test the MaxOpenTables property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the MaxOpenTables property")]
        public void SetAndRetrieveInstanceParametersMaxOpenTables()
        {
            int expected = 400;
            this.instanceParameters.MaxOpenTables = expected;
            Assert.AreEqual(expected, this.instanceParameters.MaxOpenTables);
        }

        /// <summary>
        /// Setting the MaxOpenTables property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the MaxOpenTables property should set the parameter on the instance")]
        public void VerifySetInstanceParametersMaxOpenTables()
        {
            int expected = 100;
            this.instanceParameters.MaxOpenTables = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.MaxOpenTables));
        }

        /// <summary>
        /// Test the MaxCursors property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the MaxCursors property")]
        public void SetAndRetrieveInstanceParametersMaxCursors()
        {
            int expected = 4000;
            this.instanceParameters.MaxCursors = expected;
            Assert.AreEqual(expected, this.instanceParameters.MaxCursors);
        }

        /// <summary>
        /// Setting the MaxCursors property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the MaxCursors property should set the parameter on the instance")]
        public void VerifySetInstanceParametersMaxCursors()
        {
            int expected = 64;
            this.instanceParameters.MaxCursors = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.MaxCursors));
        }

        /// <summary>
        /// Test the MaxVerPages property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the MaxVerPages property")]
        public void SetAndRetrieveInstanceParametersMaxVerPages()
        {
            int expected = 128;
            this.instanceParameters.MaxVerPages = expected;
            Assert.AreEqual(expected, this.instanceParameters.MaxVerPages);
        }

        /// <summary>
        /// Setting the MaxVerPages property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the MaxVerPages property should set the parameter on the instance")]
        public void VerifySetInstanceParametersMaxVerPages()
        {
            int expected = 128;
            this.instanceParameters.MaxVerPages = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.MaxVerPages));
        }

        /// <summary>
        /// Test the PreferredVerPages property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the PreferredVerPages property")]
        public void SetAndRetrieveInstanceParametersPreferredVerPages()
        {
            int expected = 4;
            this.instanceParameters.PreferredVerPages = expected;
            Assert.AreEqual(expected, this.instanceParameters.PreferredVerPages);
        }

        /// <summary>
        /// Setting the PreferredVerPages property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the PreferredVerPages property should set the parameter on the instance")]
        public void VerifySetInstanceParametersPreferredVerPages()
        {
            int expected = 4;
            this.instanceParameters.PreferredVerPages = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.PreferredVerPages));
        }

        /// <summary>
        /// Test the VersionStoreTaskQueueMax property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the VersionStoreTaskQueueMax property")]
        public void SetAndRetrieveInstanceParametersVersionStoreTaskQueueMax()
        {
            int expected = 12;
            this.instanceParameters.VersionStoreTaskQueueMax = expected;
            Assert.AreEqual(expected, this.instanceParameters.VersionStoreTaskQueueMax);
        }

        /// <summary>
        /// Setting the VersionStoreTaskQueueMax property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the VersionStoreTaskQueueMax property should set the parameter on the instance")]
        public void VerifySetInstanceParametersVersionStoreTaskQueueMax()
        {
            int expected = 14;
            this.instanceParameters.VersionStoreTaskQueueMax = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.VersionStoreTaskQueueMax));
        }

        /// <summary>
        /// Test the MaxTemporaryTables property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the MaxTemporaryTables property")]
        public void SetAndRetrieveInstanceParametersMaxTemporaryTables()
        {
            int expected = 7;
            this.instanceParameters.MaxTemporaryTables = expected;
            Assert.AreEqual(expected, this.instanceParameters.MaxTemporaryTables);
        }

        /// <summary>
        /// Setting the MaxTemporaryTables property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the MaxTemporaryTables property should set the parameter on the instance")]
        public void VerifySetInstanceParametersMaxTemporaryTables()
        {
            int expected = 99;
            this.instanceParameters.MaxTemporaryTables = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.MaxTemporaryTables));
        }

        /// <summary>
        /// Test the LogFileSize property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the LogFileSize property")]
        public void SetAndRetrieveInstanceParametersLogFileSize()
        {
            int expected = 4096;
            this.instanceParameters.LogFileSize = expected;
            Assert.AreEqual(expected, this.instanceParameters.LogFileSize);
        }

        /// <summary>
        /// Setting the LogFileSize property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the LogFileSize property should set the parameter on the instance")]
        public void VerifySetInstanceParametersLogFileSize()
        {
            int expected = 2048;
            this.instanceParameters.LogFileSize = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.LogFileSize));
        }

        /// <summary>
        /// Test the LogBuffers property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the LogBuffers property")]
        public void SetAndRetrieveInstanceParametersLogBuffers()
        {
            int expected = 128;
            this.instanceParameters.LogBuffers = expected;
            Assert.AreEqual(expected, this.instanceParameters.LogBuffers);
        }

        /// <summary>
        /// Setting the LogBuffers property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the LogBuffers property should set the parameter on the instance")]
        public void VerifySetInstanceParametersLogBuffers()
        {
            int expected = 256;
            this.instanceParameters.LogBuffers = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.LogBuffers));
        }

        /// <summary>
        /// Test the CircularLog property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the CircularLog property")]
        public void SetAndRetrieveInstanceParametersCircularLog()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.CircularLog = expected;
            Assert.AreEqual(expected, this.instanceParameters.CircularLog);
        }

        /// <summary>
        /// Setting the CircularLog property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the CircularLog property should set the parameter on the instance")]
        public void VerifySetInstanceParametersCircularLog()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.CircularLog = expected;
            Assert.AreEqual(expected, this.GetBooleanParameter(JET_param.CircularLog));
        }

        /// <summary>
        /// Test the CleanupMismatchedLogFiles property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the CleanupMismatchedLogFiles property")]
        public void SetAndRetrieveInstanceParametersCleanupMismatchedLogFiles()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.CleanupMismatchedLogFiles = expected;
            Assert.AreEqual(expected, this.instanceParameters.CleanupMismatchedLogFiles);
        }

        /// <summary>
        /// Setting the CleanupMismatchedLogFiles property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the CleanupMismatchedLogFiles property should set the parameter on the instance")]
        public void VerifySetInstanceParametersCleanupMismatchedLogFiles()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.CleanupMismatchedLogFiles = expected;
            Assert.AreEqual(expected, this.GetBooleanParameter(JET_param.CleanupMismatchedLogFiles));
        }

        /// <summary>
        /// Test the PageTempDBMin property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the PageTempDBMin property")]
        public void SetAndRetrieveInstanceParametersPageTempDbMin()
        {
            int expected = 100;
            this.instanceParameters.PageTempDBMin = expected;
            Assert.AreEqual(expected, this.instanceParameters.PageTempDBMin);
        }

        /// <summary>
        /// Test the PageTempDBMin property with the smallest possible value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the PageTempDBMin property with the smallest possible value")]
        public void SetAndRetrieveInstanceParametersPageTempDbMinSmallest()
        {
            int expected = SystemParameters.PageTempDBSmallest;
            this.instanceParameters.PageTempDBMin = expected;
            Assert.AreEqual(expected, this.instanceParameters.PageTempDBMin);
        }

        /// <summary>
        /// Setting the PageTempDBMin property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the PageTempDBMin property should set the parameter on the instance")]
        public void VerifySetInstanceParametersPageTempDbMin()
        {
            int expected = 128;
            this.instanceParameters.PageTempDBMin = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.PageTempDBMin));
        }

        /// <summary>
        /// Test the CheckpointDepthMax property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the CheckpointDepthMax property")]
        public void SetAndRetrieveInstanceParametersCheckpointDepthMax()
        {
            int expected = 30000;
            this.instanceParameters.CheckpointDepthMax = expected;
            Assert.AreEqual(expected, this.instanceParameters.CheckpointDepthMax);
        }

        /// <summary>
        /// Setting the CheckpointDepthMax property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the CheckpointDepthMax property should set the parameter on the instance")]
        public void VerifySetInstanceParametersCheckpointDepthMax()
        {
            int expected = 10000000;
            this.instanceParameters.CheckpointDepthMax = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.CheckpointDepthMax));
        }

        /// <summary>
        /// Test the DbExtensionSize property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the DbExtensionSize property")]
        public void SetAndRetrieveInstanceParametersDbExtensionSize()
        {
            int expected = 512;
            this.instanceParameters.DbExtensionSize = expected;
            Assert.AreEqual(expected, this.instanceParameters.DbExtensionSize);
        }

        /// <summary>
        /// Setting the DbExtensionSize property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the DbExtensionSize property should set the parameter on the instance")]
        public void VerifySetInstanceParametersDbExtensionSize()
        {
            int expected = 1024;
            this.instanceParameters.DbExtensionSize = expected;
            Assert.AreEqual(expected, this.GetIntegerParameter(JET_param.DbExtensionSize));
        }

        /// <summary>
        /// Test the CachedClosedTables property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the CachedClosedTables property")]
        public void SetAndRetrieveInstanceParametersCachedClosedTables()
        {
            if (EsentVersion.SupportsVistaFeatures)
            {
                const int Expected = 1000;
                this.instanceParameters.CachedClosedTables = Expected;
                Assert.AreEqual(Expected, this.instanceParameters.CachedClosedTables);
            }
            else
            {
                Assert.AreEqual(0, this.instanceParameters.CachedClosedTables);
            }
        }

        /// <summary>
        /// Setting the CachedClosedTables property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the CachedClosedTables property should set the parameter on the instance")]
        public void VerifySetInstanceParametersCachedClosedTables()
        {
            if (EsentVersion.SupportsVistaFeatures)
            {
                const int Expected = 2000;
                this.instanceParameters.CachedClosedTables = Expected;
                Assert.AreEqual(Expected, this.GetIntegerParameter(VistaParam.CachedClosedTables));
            }
        }

        /// <summary>
        /// Test the WaypointLatency property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the WaypointLatency property")]
        public void SetAndRetrieveInstanceParametersWaypointLatency()
        {
            if (EsentVersion.SupportsWindows7Features)
            {
                const int Expected = 10;
                this.instanceParameters.WaypointLatency = Expected;
                Assert.AreEqual(Expected, this.instanceParameters.WaypointLatency);
            }
            else
            {
                Assert.AreEqual(0, this.instanceParameters.WaypointLatency);
            }
        }

        /// <summary>
        /// Setting the WaypointLatency property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the WaypointLatency property should set the parameter on the instance")]
        public void VerifySetInstanceParametersWaypointLatency()
        {
            if (EsentVersion.SupportsWindows7Features)
            {
                const int Expected = 4;
                this.instanceParameters.WaypointLatency = Expected;
                Assert.AreEqual(Expected, this.GetIntegerParameter(Windows7Param.WaypointLatency));
            }
        }

        /// <summary>
        /// Turn on logging with the recovery property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Turn on logging with the recovery property")]
        public void SetAndRetrieveInstanceParametersRecoveryOn()
        {
            const bool Expected = true;
            this.instanceParameters.Recovery = Expected;
            Assert.AreEqual(Expected, this.instanceParameters.Recovery);
        }

        /// <summary>
        /// Turn off logging with the recovery property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Turn off logging with the recovery property")]
        public void SetAndRetrieveInstanceParametersRecoveryOff()
        {
            const bool Expected = false;
            this.instanceParameters.Recovery = Expected;
            Assert.AreEqual(Expected, this.instanceParameters.Recovery);
        }

        /// <summary>
        /// Test the EnableIndexChecking property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the EnableIndexChecking property")]
        public void SetAndRetrieveInstanceParametersEnableIndexChecking()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.EnableIndexChecking = expected;
            Assert.AreEqual(expected, this.instanceParameters.EnableIndexChecking);
        }

        /// <summary>
        /// Setting the EnableIndexChecking property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the EnableIndexChecking property should set the parameter on the instance")]
        public void VerifySetInstanceParametersEnableIndexChecking()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.EnableIndexChecking = expected;
            Assert.AreEqual(expected, this.GetBooleanParameter(JET_param.EnableIndexChecking));
        }

        /// <summary>
        /// Test setting the no information event property to true.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the no information event property to true")]
        public void SetAndRetrieveInstanceParametersNoInformationEventOn()
        {
            this.instanceParameters.NoInformationEvent = true;
            Assert.IsTrue(this.instanceParameters.NoInformationEvent);
        }

        /// <summary>
        /// Test setting the no information event property to false.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the no information event property to false")]
        public void SetAndRetrieveInstanceParametersNoInformationEventOff()
        {
            this.instanceParameters.NoInformationEvent = false;
            Assert.IsFalse(this.instanceParameters.NoInformationEvent);
        }

        /// <summary>
        /// Setting the NoInformationEvent property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the NoInformationEvent property should set the parameter on the instance")]
        public void VerifySetInstanceParametersNoInformationEvent()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.NoInformationEvent = expected;
            Assert.AreEqual(expected, this.GetBooleanParameter(JET_param.NoInformationEvent));
        }

        /// <summary>
        /// Test setting the one database per session property to true.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the one database per session property to true")]
        public void SetAndRetrieveInstanceParametersOneDatabasePerSessionOn()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                // Setting this instance per-parameter isn't supported.
                return;
            }

            this.instanceParameters.OneDatabasePerSession = true;
            Assert.IsTrue(this.instanceParameters.OneDatabasePerSession);
        }

        /// <summary>
        /// Test setting the one database per session property to false.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test setting the one database per session property to false")]
        public void SetAndRetrieveInstanceParametersOneDatabasePerSessionOff()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                // Setting this instance per-parameter isn't supported.
                return;
            }

            this.instanceParameters.OneDatabasePerSession = false;
            Assert.IsFalse(this.instanceParameters.OneDatabasePerSession);
        }

        /// <summary>
        /// Setting the OneDatabasePerSession property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the OneDatabasePerSession property should set the parameter on the instance")]
        public void VerifySetInstanceParametersOneDatabasePerSession()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                // Setting this instance per-parameter isn't supported.
                return;
            }

            bool expected = Any.Boolean;
            this.instanceParameters.OneDatabasePerSession = expected;
            Assert.AreEqual(expected, this.GetBooleanParameter(JET_param.OneDatabasePerSession));
        }

        /// <summary>
        /// Test the create path if not exist property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the create path if not exist property")]
        public void SetAndRetrieveInstanceParametersCreatePathIfNotExist()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.CreatePathIfNotExist = expected;
            Assert.AreEqual(expected, this.instanceParameters.CreatePathIfNotExist);
        }

        /// <summary>
        /// Setting the CreatePathIfNotExist property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the CreatePathIfNotExist property should set the parameter on the instance")]
        public void VerifySetInstanceParametersCreatePathIfNotExist()
        {
            bool expected = Any.Boolean;
            this.instanceParameters.CreatePathIfNotExist = expected;
            Assert.AreEqual(expected, this.GetBooleanParameter(JET_param.CreatePathIfNotExist));
        }

        /// <summary>
        /// Test the temporary directory property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the temporary directory property")]
        public void SetAndRetrieveInstanceParametersTempDirectory()
        {
            string dir = @"c:\foo\";
            this.instanceParameters.TempDirectory = dir;
            Assert.AreEqual(dir, this.instanceParameters.TempDirectory);
        }

        /// <summary>
        /// Setting the TempDirectory property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the TempDirectory property should set the parameter on the instance")]
        public void VerifySetInstanceParametersTempDirectory()
        {
            string dir = @"g:\mydir\";
            this.instanceParameters.TempDirectory = dir;
            StringAssert.StartsWith(this.GetStringParameter(JET_param.TempPath), dir);
        }

        /// <summary>
        /// Test the temporary directory property without a trailing slash.
        /// Make sure the slash is added when retrieving it.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the temporary directory property without a trailing slash")]
        public void SetAndRetrieveInstanceParametersTempDirectoryAddsSeparatorChar()
        {
            this.instanceParameters.TempDirectory = @"c:\bar\baz";
            Assert.AreEqual(@"c:\bar\baz\", this.instanceParameters.TempDirectory);
        }

        /// <summary>
        /// Test the log directory property. The trailing slash will be added.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the log directory property")]
        public void SetAndRetrieveInstanceParametersLogFileDirectory()
        {
            string dir = @"d:\logs";
            this.instanceParameters.LogFileDirectory = dir;
            Assert.AreEqual(@"d:\logs\", this.instanceParameters.LogFileDirectory);
        }

        /// <summary>
        /// Setting the LogFileDirectory property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the LogFileDirectory property should set the parameter on the instance")]
        public void VerifySetInstanceParametersLogFileDirectory()
        {
            string dir = @"g:\mydir\";
            this.instanceParameters.LogFileDirectory = dir;
            StringAssert.StartsWith(this.GetStringParameter(JET_param.LogFilePath), dir);
        }

        /// <summary>
        /// Test the system directory property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the system directory property")]
        public void SetAndRetrieveInstanceParametersSystemDirectory()
        {
            string dir = @"d:\a\b\c\system\";
            this.instanceParameters.SystemDirectory = dir;
            Assert.AreEqual(@"d:\a\b\c\system\", this.instanceParameters.SystemDirectory);
        }

        /// <summary>
        /// Setting the SystemDirectory property should set the parameter
        /// on the instance.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Setting the SystemDirectory property should set the parameter on the instance")]
        public void VerifySetInstanceParametersSystemDirectory()
        {
            string dir = @"g:\mydir\";
            this.instanceParameters.SystemDirectory = dir;
            StringAssert.StartsWith(this.GetStringParameter(JET_param.SystemPath), dir);
        }
        
        /// <summary>
        /// Test the alternate database recovery directory.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test the alternate database recovery directory")]
        public void SetAndRetrieveInstanceParametersAlternateDatabaseRecoveryDirectory()
        {
            if (EsentVersion.SupportsServer2003Features)
            {
                string dir = @"z:\my\db";
                this.instanceParameters.AlternateDatabaseRecoveryDirectory = dir;
                Assert.AreEqual(@"z:\my\db\", this.instanceParameters.AlternateDatabaseRecoveryDirectory);
            }
        }

        #region Helper Methods

        /// <summary>
        /// Retrieve a string parameter.
        /// </summary>
        /// <param name="param">The parameter to retrieve.</param>
        /// <returns>The parameter value.</returns>
        private string GetStringParameter(JET_param param)
        {
            int ignored = 0;
            string value;

            Api.JetGetSystemParameter(this.instance, JET_SESID.Nil, param, ref ignored, out value, 1024);
            return value;
        }

        /// <summary>
        /// Retrieve an integer parameter.
        /// </summary>
        /// <param name="param">The parameter to retrieve.</param>
        /// <returns>The parameter value.</returns>
        private int GetIntegerParameter(JET_param param)
        {
            int value = 0;
            string ignored;

            Api.JetGetSystemParameter(this.instance, JET_SESID.Nil, param, ref value, out ignored, 0);
            return value;
        }

        /// <summary>
        /// Retrieve a boolean parameter.
        /// </summary>
        /// <param name="param">The parameter to retrieve.</param>
        /// <returns>The parameter value.</returns>
        private bool GetBooleanParameter(JET_param param)
        {
            int value = 0;
            string ignored;

            Api.JetGetSystemParameter(this.instance, JET_SESID.Nil, param, ref value, out ignored, 0);
            return 0 != value;
        }

        #endregion Helper Methods
    }
}
