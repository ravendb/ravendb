//-----------------------------------------------------------------------
// <copyright file="DisposableObjectIdTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the disposable object id
    /// </summary>
    [TestClass]
    public class DisposableObjectIdTests
    {
        #region CompareTo

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdCompareToReturnsZeroForEqualIds()
        {
            DisposableObjectId id1 = DisposableObjectManager.GetNewDisposableObjectId();
            Assert.AreEqual(0, id1.CompareTo(id1));
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdCompareToReturnsOneForLessThan()
        {
            var id1 = new DisposableObjectId() { Value = 1 };
            var id2 = new DisposableObjectId() { Value = 2 };
            Assert.AreEqual(-1, id1.CompareTo(id2));
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdCompareToReturnsMinusOneForGreaterThan()
        {
            var id1 = new DisposableObjectId() { Value = 5 };
            var id2 = new DisposableObjectId() { Value = 4 };
            Assert.AreEqual(1, id1.CompareTo(id2));
        }

        #endregion

        #region operator<

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdLessThanReturnsTrueWhenLessThan()
        {
            var id1 = new DisposableObjectId() { Value = 2 };
            var id2 = new DisposableObjectId() { Value = 3 };
            Assert.IsTrue(id1 < id2);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdLessThanReturnsFalseWhenEqual()
        {
            var id1 = new DisposableObjectId() { Value = 2 };
            var id2 = new DisposableObjectId() { Value = 2 };
            Assert.IsFalse(id1 < id2);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdLessThanReturnsFalseWhenGreaterThan()
        {
            var id1 = new DisposableObjectId() { Value = 3 };
            var id2 = new DisposableObjectId() { Value = 2 };
            Assert.IsFalse(id1 < id2);
        }

        #endregion

        #region operator>

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdGreaterThanReturnsFalseWhenLessThan()
        {
            var id1 = new DisposableObjectId() { Value = 2 };
            var id2 = new DisposableObjectId() { Value = 3 };
            Assert.IsFalse(id1 > id2);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdGreaterThanReturnsFalseWhenEqual()
        {
            var id1 = new DisposableObjectId() { Value = 2 };
            var id2 = new DisposableObjectId() { Value = 2 };
            Assert.IsFalse(id1 < id2);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectIdGreaterThanReturnsTrueWhenGreaterThan()
        {
            var id1 = new DisposableObjectId() { Value = 3 };
            var id2 = new DisposableObjectId() { Value = 2 };
            Assert.IsTrue(id1 > id2);
        }

        #endregion
    }
}