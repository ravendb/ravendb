//-----------------------------------------------------------------------
// <copyright file="ConversionsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Globalization;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the methods of the Conversions class
    /// </summary>
    [TestClass]
    public class ConversionsTests
    {
        /// <summary>
        /// Test basic conversion of a double to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting a double to a DateTime")]
        public void TestConvertDoubleToDateTime()
        {
            DateTime date = Any.DateTime;
            Assert.AreEqual(date, Conversions.ConvertDoubleToDateTime(date.ToOADate())); 
        }

        /// <summary>
        /// Test conversion of Double.MinValue to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting Double.MinValue to a DateTime")]
        public void TestConvertMinValueToDateTime()
        {
            Assert.AreEqual(DateTime.MinValue, Conversions.ConvertDoubleToDateTime(Double.MinValue));
        }

        /// <summary>
        /// Test conversion of Double.MaxValue to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting Double.MaxValue to a DateTime")]
        public void TestConvertMaxValueToDateTime()
        {
            Assert.AreEqual(DateTime.MaxValue, Conversions.ConvertDoubleToDateTime(Double.MaxValue));
        }

        /// <summary>
        /// Converting default (0) LCMapFlags should return CompareOptions.None.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting default LCMapFlags")]
        public void ConvertDefaultLCMapFlags()
        {
            Assert.AreEqual(CompareOptions.None, Conversions.CompareOptionsFromLCMapFlags(0));
        }

        /// <summary>
        /// Convert one LCMapFlag
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting one LCMapFlag")]
        public void ConvertOneLCMapFlag()
        {
            uint flags = 0x01; // NORM_IGNORECASE
            Assert.AreEqual(CompareOptions.IgnoreCase, Conversions.CompareOptionsFromLCMapFlags(flags));
        }

        /// <summary>
        /// Convert multiple LCMapFlags
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting multiple LCMapFlags")]
        public void ConvertMultipleLCMapFlags()
        {
            uint flags = 0x6; // NORM_IGNORENONSPACE | NORM_IGNORESYMBOLS
            Assert.AreEqual(
                CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreSymbols,
                Conversions.CompareOptionsFromLCMapFlags(flags));
        }

        /// <summary>
        /// Convert unknown LCMapFlags
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting an unknown LCMapFlag")]
        public void ConvertUnknownLCMapFlags()
        {
            uint flags = 0x8020000; // NORM_LINGUISTIC_CASING | NORM_IGNOREWIDTH
            Assert.AreEqual(
                CompareOptions.IgnoreWidth,
                Conversions.CompareOptionsFromLCMapFlags(flags));
        }

        /// <summary>
        /// Converting CompareOptions.None should return 0.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting CompareOptions.None")]
        public void ConvertDefaultCompareOptions()
        {
            uint flags = 0;
            Assert.AreEqual(flags, Conversions.LCMapFlagsFromCompareOptions(CompareOptions.None));
        }

        /// <summary>
        /// Convert one CompareOption
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting one CompareOption (CompareOptions.IgnoreCase)")]
        public void ConvertOneCompareOption()
        {
            uint flags = 0x1; // NORM_IGNORECASE
            Assert.AreEqual(flags, Conversions.LCMapFlagsFromCompareOptions(CompareOptions.IgnoreCase));
        }

        /// <summary>
        /// Convert multiple CompareOptions
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting multiple CompareOptions")]
        public void ConvertMultipleCompareOptions()
        {
            uint flags = 0x6; // NORM_IGNORENONSPACE | NORM_IGNORESYMBOLS
            Assert.AreEqual(flags, Conversions.LCMapFlagsFromCompareOptions(CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreSymbols));
        }

        /// <summary>
        /// Convert unknown CompareOption
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting an unknown CompareOption")]
        public void ConvertUnknownCompareOptions()
        {
            uint flags = 0;
            Assert.AreEqual(flags, Conversions.LCMapFlagsFromCompareOptions(CompareOptions.Ordinal));
        }
    }
}