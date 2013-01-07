//-----------------------------------------------------------------------
// <copyright file="DbutilTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test Dbutil methods.
    /// </summary>
    [TestClass()]
    public class DbutilTests
    {
        /// <summary>
        /// Test FormatBytes.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void FormatBytes()
        {
            var data = new byte[] { 0x1, 0xf, 0x22, 0xee };
            const string Expected = "010f22ee";
            string actual = Dbutil.FormatBytes(data);
            Assert.AreEqual(Expected, actual);
        }

        /// <summary>
        /// Test FormatBytes with null data.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void FormatBytesWithNullData()
        {
            Assert.IsNull(Dbutil.FormatBytes(null));
        }

        /// <summary>
        /// Test FormatBytes with zero-length data.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void FormatBytesWithZeroLengthData()
        {
            var data = new byte[0];
            string expected = String.Empty;
            string actual = Dbutil.FormatBytes(data);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test QuoteForCsv with text that is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvNullText()
        {
            Assert.IsNull(Dbutil.QuoteForCsv(null));
        }

        /// <summary>
        /// Test QuoteForCsv with text that is empty.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvEmptyText()
        {
            Assert.AreEqual(String.Empty, Dbutil.QuoteForCsv(String.Empty));
        }

        /// <summary>
        /// Test QuoteForCsv with text that doesn't need quoting.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvNoQuote()
        {
            Assert.AreEqual("100", Dbutil.QuoteForCsv("100"));
        }

        /// <summary>
        /// Test QuoteForCsv with text that is a single character.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvSingleChar()
        {
            Assert.AreEqual("Q", Dbutil.QuoteForCsv("Q"));
        }

        /// <summary>
        /// Test QuoteForCsv with text that contains quotes.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvContainsQuotes()
        {
            // The quote should be doubled and the text surround with quotes
            Assert.AreEqual("\"xx\"\"xx\"", Dbutil.QuoteForCsv("xx\"xx"));
        }

        /// <summary>
        /// Test QuoteForCsv with text that contains a comma.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvContainsComma()
        {
            Assert.AreEqual("\",\"", Dbutil.QuoteForCsv(","));
        }

        /// <summary>
        /// Test QuoteForCsv with text that contains a newline.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvContainsNewLine()
        {
            Assert.AreEqual("\"\r\n\"", Dbutil.QuoteForCsv("\r\n"));
        }

        /// <summary>
        /// Test QuoteForCsv with text that starts with a space.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvStartsWithSpace()
        {
            Assert.AreEqual("\" hello\"", Dbutil.QuoteForCsv(" hello"));
        }

        /// <summary>
        /// Test QuoteForCsv with text that starts with a tab.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvStartsWithTab()
        {
            Assert.AreEqual("\"\t$$\"", Dbutil.QuoteForCsv("\t$$"));
        }

        /// <summary>
        /// Test QuoteForCsv with text that ends with a space.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvEndsWithSpace()
        {
            Assert.AreEqual("\"1.2 \"", Dbutil.QuoteForCsv("1.2 "));
        }

        /// <summary>
        /// Test QuoteForCsv with text that ends with a tab.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void QuoteForCsvEndsWithTab()
        {
            Assert.AreEqual("\"__\t\"", Dbutil.QuoteForCsv("__\t"));
        }
    }
}
