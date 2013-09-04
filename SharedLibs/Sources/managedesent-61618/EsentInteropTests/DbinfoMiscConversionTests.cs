//-----------------------------------------------------------------------
// <copyright file="DbinfoMiscConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JET_DBINFOMISC.SetFromNativeDbinfoMisc.
    /// </summary>
    [TestClass]
    public class DbinfoMiscConversionTests
    {
        /// <summary>
        /// The native structure we will set values from.
        /// </summary>
        private NATIVE_DBINFOMISC4 native;

        /// <summary>
        /// The managed structure we will set.
        /// </summary>
        private JET_DBINFOMISC managed;

        /// <summary>
        /// Create the native and managed objects.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.native = new NATIVE_DBINFOMISC4
            {
                dbinfo = new NATIVE_DBINFOMISC
                {
                    ulVersion = Any.UInt32,
                    ulUpdate = Any.UInt32,
                    signDb = new NATIVE_SIGNATURE
                    {
                        logtimeCreate = Any.Logtime,
                        ulRandom = Any.UInt32,
                    },
                    dbstate = 1,
                    lgposConsistent = Any.Lgpos,
                    logtimeConsistent = Any.Logtime,
                    logtimeAttach = Any.Logtime,
                    lgposAttach = Any.Lgpos,
                    logtimeDetach = Any.Logtime,
                    lgposDetach = Any.Lgpos,
                    signLog = new NATIVE_SIGNATURE
                    {
                        logtimeCreate = Any.Logtime,
                        ulRandom = Any.UInt32,
                    },
                    bkinfoFullPrev = Any.Bkinfo,
                    bkinfoIncPrev = Any.Bkinfo,
                    bkinfoFullCur = Any.Bkinfo,
                    fShadowingDisabled = Any.UInt32,
                    fUpgradeDb = Any.UInt32,
                    dwMajorVersion = Any.UInt16,
                    dwMinorVersion = Any.UInt16,
                    dwBuildNumber = Any.UInt16,
                    lSPNumber = Any.UInt16,
                    cbPageSize = Any.UInt16,
                },
                genMinRequired = Any.UInt16,
                genMaxRequired = Any.UInt16,
                logtimeGenMaxCreate = Any.Logtime,
                ulRepairCount = Any.UInt16,
                logtimeRepair = Any.Logtime,
                ulRepairCountOld = Any.UInt16,
                ulECCFixSuccess = Any.UInt16,
                logtimeECCFixSuccess = Any.Logtime,
                ulECCFixSuccessOld = Any.UInt16,
                ulECCFixFail = Any.UInt16,
                logtimeECCFixFail = Any.Logtime,
                ulECCFixFailOld = Any.UInt16,
                ulBadChecksum = Any.UInt16,
                logtimeBadChecksum = Any.Logtime,
                ulBadChecksumOld = Any.UInt16,
                genCommitted = Any.UInt16,
                bkinfoCopyPrev = Any.Bkinfo,
                bkinfoDiffPrev = Any.Bkinfo,
            };

            this.managed = new JET_DBINFOMISC();
            this.managed.SetFromNativeDbinfoMisc(ref this.native);
        }

        /// <summary>
        /// Verify that ulVersion is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulVersion is set")]
        public void VerifyUlVersion()
        {
            Assert.AreEqual((int)this.native.dbinfo.ulVersion, this.managed.ulVersion);
        }

        /// <summary>
        /// Verify that ulUpdate is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulUpdate is set")]
        public void VerifyUlUpdate()
        {
            Assert.AreEqual((int)this.native.dbinfo.ulUpdate, this.managed.ulUpdate);
        }

        /// <summary>
        /// Verify that signDb is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that signDb is set")]
        public void VerifySignDb()
        {
            var expected = new JET_SIGNATURE(this.native.dbinfo.signDb);
            Assert.AreEqual(expected, this.managed.signDb);
        }

        /// <summary>
        /// Verify that dbstate is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that dbstate is set")]
        public void VerifyDbstate()
        {
            Assert.AreEqual((JET_dbstate)this.native.dbinfo.dbstate, this.managed.dbstate);
        }

        /// <summary>
        /// Verify that lgposConsistent is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that lgposConsistent is set")]
        public void VerifyLgposConsistent()
        {
            Assert.AreEqual(this.native.dbinfo.lgposConsistent, this.managed.lgposConsistent);
        }

        /// <summary>
        /// Verify that logtimeConsistent is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeConsistent is set")]
        public void VerifyLogtimeConsistent()
        {
            Assert.AreEqual(this.native.dbinfo.logtimeConsistent, this.managed.logtimeConsistent);
        }

        /// <summary>
        /// Verify that logtimeAttach is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeAttach is set")]
        public void VerifyLogtimeAttach()
        {
            Assert.AreEqual(this.native.dbinfo.logtimeAttach, this.managed.logtimeAttach);
        }

        /// <summary>
        /// Verify that lgposAttach is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that lgposAttach is set")]
        public void VerifyLgposAttach()
        {
            Assert.AreEqual(this.native.dbinfo.lgposAttach, this.managed.lgposAttach);
        }

        /// <summary>
        /// Verify that logtimeDetach is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeDetach is set")]
        public void VerifyLogtimeDetach()
        {
            Assert.AreEqual(this.native.dbinfo.logtimeDetach, this.managed.logtimeDetach);
        }

        /// <summary>
        /// Verify that lgposDetach is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that lgposDetach is set")]
        public void VerifyLgposDetach()
        {
            Assert.AreEqual(this.native.dbinfo.lgposDetach, this.managed.lgposDetach);
        }

        /// <summary>
        /// Verify that signLog is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that signLog is set")]
        public void VerifySignLog()
        {
            var expected = new JET_SIGNATURE(this.native.dbinfo.signLog);
            Assert.AreEqual(expected, this.managed.signLog);
        }

        /// <summary>
        /// Verify that bkinfoFullPrev is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that bkinfoFullPrev is set")]
        public void VerifyBkinfoFullPrev()
        {
            Assert.AreEqual(this.native.dbinfo.bkinfoFullPrev, this.managed.bkinfoFullPrev);
        }

        /// <summary>
        /// Verify that bkinfoIncPrev is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that bkinfoIncPrev is set")]
        public void VerifyBkinfoIncPrev()
        {
            Assert.AreEqual(this.native.dbinfo.bkinfoIncPrev, this.managed.bkinfoIncPrev);
        }

        /// <summary>
        /// Verify that bkinfoFullCur is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that bkinfoFullCur is set")]
        public void VerifyBkinfoFullCur()
        {
            Assert.AreEqual(this.native.dbinfo.bkinfoFullCur, this.managed.bkinfoFullCur);
        }

        /// <summary>
        /// Verify that fShadowingDisabled is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that fShadowingDisabled is set")]
        public void VerifyFShadowingDisabled()
        {
            Assert.AreEqual(0 == this.native.dbinfo.fShadowingDisabled ? false : true, this.managed.fShadowingDisabled);
        }

        /// <summary>
        /// Verify that fUpgradeDb is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that fUpgradeDb is set")]
        public void VerifyFUpgradeDb()
        {
            Assert.AreEqual(0 == this.native.dbinfo.fUpgradeDb ? false : true, this.managed.fUpgradeDb);
        }

        /// <summary>
        /// Verify that dwMajorVersion is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that dwMajorVersion is set")]
        public void VerifyDwMajorVersion()
        {
            Assert.AreEqual((int)this.native.dbinfo.dwMajorVersion, this.managed.dwMajorVersion);
        }

        /// <summary>
        /// Verify that dwMinorVersion is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that dwMinorVersion is set")]
        public void VerifyDwMinorVersion()
        {
            Assert.AreEqual((int)this.native.dbinfo.dwMinorVersion, this.managed.dwMinorVersion);
        }

        /// <summary>
        /// Verify that dwBuildNumber is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that dwBuildNumber is set")]
        public void VerifyDwBuildNumber()
        {
            Assert.AreEqual((int)this.native.dbinfo.dwBuildNumber, this.managed.dwBuildNumber);
        }

        /// <summary>
        /// Verify that lSPNumber is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that lSpNumber is set")]
        public void VerifylSpNumber()
        {
            Assert.AreEqual((int)this.native.dbinfo.lSPNumber, this.managed.lSPNumber);
        }

        /// <summary>
        /// Verify that cbPageSize is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that cbPageSize is set")]
        public void VerifyCbPageSize()
        {
            Assert.AreEqual((int)this.native.dbinfo.cbPageSize, this.managed.cbPageSize);
        }

        /// <summary>
        /// Verify that genMinRequired is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that genMinRequired is set")]
        public void VerifyGenMinRequired()
        {
            Assert.AreEqual((int)this.native.genMinRequired, this.managed.genMinRequired);
        }

        /// <summary>
        /// Verify that genMaxRequired is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that genMaxRequired is set")]
        public void VerifyGenMaxRequired()
        {
            Assert.AreEqual((int)this.native.genMaxRequired, this.managed.genMaxRequired);
        }

        /// <summary>
        /// Verify that logtimeGenMaxCreate is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeGenMaxCreate is set")]
        public void VerifyLogtimeGenMaxCreate()
        {
            Assert.AreEqual(this.native.logtimeGenMaxCreate, this.managed.logtimeGenMaxCreate);
        }

        /// <summary>
        /// Verify that ulRepairCount is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulRepairCount is set")]
        public void VerifyUlRepairCount()
        {
            Assert.AreEqual((int)this.native.ulRepairCount, this.managed.ulRepairCount);
        }

        /// <summary>
        /// Verify that logtimeRepair is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeRepair is set")]
        public void VerifyLogtimeRepair()
        {
            Assert.AreEqual(this.native.logtimeRepair, this.managed.logtimeRepair);
        }

        /// <summary>
        /// Verify that ulRepairCountOld is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulRepairCountOld is set")]
        public void VerifyUlRepairCountOld()
        {
            Assert.AreEqual((int)this.native.ulRepairCountOld, this.managed.ulRepairCountOld);
        }

        /// <summary>
        /// Verify that ulECCFixSuccess is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulECCFixSuccess is set")]
        public void VerifyUlEccFixSuccess()
        {
            Assert.AreEqual((int)this.native.ulECCFixSuccess, this.managed.ulECCFixSuccess);
        }

        /// <summary>
        /// Verify that logtimeECCFixSuccess is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeECCFixSuccess is set")]
        public void VerifyLogtimeEccFixSuccess()
        {
            Assert.AreEqual(this.native.logtimeECCFixSuccess, this.managed.logtimeECCFixSuccess);
        }

        /// <summary>
        /// Verify that ulECCFixSuccessOld is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulECCFixSuccessOld is set")]
        public void VerifyUlEccFixSuccessOld()
        {
            Assert.AreEqual((int)this.native.ulECCFixSuccessOld, this.managed.ulECCFixSuccessOld);
        }

        /// <summary>
        /// Verify that ulECCFixFail is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulECCFixFail is set")]
        public void VerifyUlEccFixFail()
        {
            Assert.AreEqual((int)this.native.ulECCFixFail, this.managed.ulECCFixFail);
        }

        /// <summary>
        /// Verify that logtimeECCFixFail is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeECCFixFail is set")]
        public void VerifyLogtimeEccFixFail()
        {
            Assert.AreEqual(this.native.logtimeECCFixFail, this.managed.logtimeECCFixFail);
        }

        /// <summary>
        /// Verify that ulECCFixFailOld is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulECCFixFailOld is set")]
        public void VerifyUlEccFixFailOld()
        {
            Assert.AreEqual((int)this.native.ulECCFixFailOld, this.managed.ulECCFixFailOld);
        }

        /// <summary>
        /// Verify that ulBadChecksum is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulBadChecksum is set")]
        public void VerifyUlBadChecksum()
        {
            Assert.AreEqual((int)this.native.ulBadChecksum, this.managed.ulBadChecksum);
        }

        /// <summary>
        /// Verify that logtimeBadChecksum is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that logtimeBadChecksum is set")]
        public void VerifyLogtimeBadChecksum()
        {
            Assert.AreEqual(this.native.logtimeBadChecksum, this.managed.logtimeBadChecksum);
        }

        /// <summary>
        /// Verify that ulBadChecksumOld is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that ulBadChecksumOld is set")]
        public void VerifyUlBadChecksumOld()
        {
            Assert.AreEqual((int)this.native.ulBadChecksumOld, this.managed.ulBadChecksumOld);
        }

        /// <summary>
        /// Verify that genCommitted is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that genCommitted is set")]
        public void VerifyGenCommitted()
        {
            Assert.AreEqual((int)this.native.genCommitted, this.managed.genCommitted);
        }

        /// <summary>
        /// Verify that bkinfoCopyPrev is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that bkinfoCopyPrev is set")]
        public void VerifyBkinfoCopyPrev()
        {
            Assert.AreEqual(this.native.bkinfoCopyPrev, this.managed.bkinfoCopyPrev);
        }

        /// <summary>
        /// Verify that bkinfoDiffPrev is set.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that bkinfoDiffPrev is set")]
        public void VerifyBkinfoDiffPrev()
        {
            Assert.AreEqual(this.native.bkinfoDiffPrev, this.managed.bkinfoDiffPrev);
        }

        /// <summary>
        /// Verify that GetNativeDbinfomisc4 works.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that GetNativeDbinfomisc4 works.")]
        public void VerifyGetNativeDbInfoMisc()
        {
            // We need to test GetNativeDbinfomisc4
            var nativeDbinfoMisc4 = this.managed.GetNativeDbinfomisc4();
            var reconstituted = new JET_DBINFOMISC();
            reconstituted.SetFromNativeDbinfoMisc(ref nativeDbinfoMisc4);
            Assert.IsTrue(reconstituted.Equals(this.managed));
        }

        /// <summary>
        /// Verify that GetNativeDbinfomisc4 works with some variables complemented.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that GetNativeDbinfomisc4 works with some variables complemented.")]
        public void VerifyGetNativeDbInfoMiscWithaLthernate()
        {
            // We need to test GetNativeDbinfomisc4 with some alternate values.
            // In order to do that, we need to change some variables around,
            // but this is an easy (if convoluted!) way of duplicating the object.
            var nativeDbinfoMisc4 = this.managed.GetNativeDbinfomisc4();
            var reconstituted = new JET_DBINFOMISC();
            reconstituted.SetFromNativeDbinfoMisc(ref nativeDbinfoMisc4);
            Assert.IsTrue(reconstituted.Equals(this.managed));

            // Now change some variables to exercise other code paths.
            reconstituted.fUpgradeDb = !reconstituted.fUpgradeDb;
            reconstituted.fShadowingDisabled = !reconstituted.fShadowingDisabled;
            var nativeOpposite = reconstituted.GetNativeDbinfomisc4();
            var managedOpposite = new JET_DBINFOMISC();
            managedOpposite.SetFromNativeDbinfoMisc(ref nativeOpposite);
            Assert.IsTrue(reconstituted.Equals(managedOpposite));
        }
    }
}