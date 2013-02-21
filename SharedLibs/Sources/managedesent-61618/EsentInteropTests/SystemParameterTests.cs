//-----------------------------------------------------------------------
// <copyright file="SystemParameterTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Rhino.Mocks;

    /// <summary>
    /// Test the SystemParameters class. To avoid changing global parameters
    /// this is tested with a mock IJetApi.
    /// </summary>
    [TestClass]
    public partial class SystemParameterTests
    {
        /// <summary>
        /// Mock object repository.
        /// </summary>
        private MockRepository repository;

        /// <summary>
        /// The real IJetApi, saved in Setup and restored in Teardown.
        /// </summary>
        private IJetApi savedApi;

        /// <summary>
        /// Mock API object.
        /// </summary>
        private IJetApi mockApi;

        /// <summary>
        /// Initialization method. Setup the mock API.
        /// </summary>
        [TestInitialize]
        [Description("Setup the SystemParameterTests test fixture")]
        public void Setup()
        {
            this.savedApi = Api.Impl;
            this.repository = new MockRepository();
            this.mockApi = this.repository.DynamicMock<IJetApi>();

            var mockCapabilities = new JetCapabilities
                {
                    SupportsLargeKeys = true,
                    SupportsUnicodePaths = true,
                    SupportsVistaFeatures = true,
                    SupportsWindows7Features = true,
                };
            SetupResult.For(this.mockApi.Capabilities).Return(mockCapabilities);

            Api.Impl = this.mockApi;
        }

        /// <summary>
        /// Cleanup after a test. This restores the saved API.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the SystemParameterTests test fixture")]
        public void Teardown()
        {
            Api.Impl = this.savedApi;
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify SystemParameters.CacheSizeMax sets JET_param.CacheSizeMax")]
        public void VerifySettingCacheSizeMax()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.CacheSizeMax, new IntPtr(64), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.CacheSizeMax = 64;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.CacheSize sets JET_param.CacheSize")]
        public void VerifySettingCacheSize()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.CacheSize, new IntPtr(64), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.CacheSize = 64;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.CacheSizeMin sets JET_param.CacheSizeMin")]
        public void VerifySettingCacheSizeMin()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.CacheSizeMin, new IntPtr(64), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.CacheSizeMin = 64;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.StartFlushThreshold sets JET_param.StartFlushThreshold")]
        public void VerifySettingStartFlushThreshold()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.StartFlushThreshold, new IntPtr(65), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.StartFlushThreshold = 65;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.StopFlushThreshold sets JET_param.StopFlushThreshold")]
        public void VerifySettingStopFlushThreshold()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.StopFlushThreshold, new IntPtr(66), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.StopFlushThreshold = 66;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.DatabasePageSize sets JET_param.DatabasePageSize")]
        public void VerifySettingDatabasePageSize()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.DatabasePageSize, new IntPtr(4096), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.DatabasePageSize = 4096;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.MaxInstances sets JET_param.MaxInstances")]
        public void VerifySettingMaxInstances()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.MaxInstances, new IntPtr(12), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.MaxInstances = 12;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.EventLoggingLevel sets JET_param.EventLoggingLevel")]
        public void VerifySettingEventLoggingLevel()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.EventLoggingLevel, new IntPtr(13), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.EventLoggingLevel = 13;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.Configuration sets VistaParam.Configuration")]
        public void VerifySettingConfiguration()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, VistaParam.Configuration, new IntPtr(0), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.Configuration = 0;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.EnableAdvanced to true sets VistaParam.EnableAdvanced")]
        public void VerifySettingEnableAdvancedToTrue()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, VistaParam.EnableAdvanced, new IntPtr(1), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.EnableAdvanced = true;
            this.repository.VerifyAll();
        }

        /// <summary>
        /// Verify that setting the property sets the system parameter
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that setting SystemParameters.Configuration to false sets VistaParam.Configuration")]
        public void VerifySettingEnableAdvancedToFalse()
        {
            Expect.Call(
                this.mockApi.JetSetSystemParameter(
                    JET_INSTANCE.Nil, JET_SESID.Nil, VistaParam.EnableAdvanced, new IntPtr(0), null)).Return(1);
            this.repository.ReplayAll();
            SystemParameters.EnableAdvanced = false;
            this.repository.VerifyAll();
        }
    }
}
