//-----------------------------------------------------------------------
// <copyright file="InstanceMiscInfoTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JetGetInstanceMiscInfo
    /// </summary>
    [TestClass]
    public class InstanceMiscInfoTests
    {
        /// <summary>
        /// The instance.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// Directory containing the logfiles.
        /// </summary>
        private string directory;

        /// <summary>
        /// Setup the InstanceMiscInfoTests fixture.
        /// </summary>
        [TestInitialize]
        [Description("Setup the InstanceMiscInfoTests fixture")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.instance = SetupHelper.CreateNewInstance(this.directory);
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
            Api.JetInit(ref this.instance);
        }

        /// <summary>
        /// Cleanup the InstanceMiscInfoTests fixture.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the InstanceMiscInfoTests fixture")]
        public void Teardown()
        {
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        /// <summary>
        /// Verify that JetGetInstanceMiscInfo does not return null.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that JetGetInstanceMiscInfo does not return null")]
        public void VerifyJetGetInstanceMiscInfoDoesNotReturnNull()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            JET_SIGNATURE signature;
            VistaApi.JetGetInstanceMiscInfo(this.instance, out signature, JET_InstanceMiscInfo.LogSignature);
            Assert.AreNotEqual(default(JET_SIGNATURE), signature);
        }
    }
}
