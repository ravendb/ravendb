//-----------------------------------------------------------------------
// <copyright file="WeakReferenceOfTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    [TestClass]
    public class WeakReferenceOfTests
    {
        [TestMethod]
        [Priority(1)]
        public void VerifyIdReturnsObjectId()
        {
            var obj = new ManagedDisposableObject();
            var weakref = new WeakReferenceOf<ManagedDisposableObject>(obj);
            Assert.AreEqual(obj.Id, weakref.Id);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyIdReturnsObjectIdWhenObjectIsFinalized()
        {
            var obj = new ManagedDisposableObject();
            DisposableObjectId id = obj.Id;
            var weakref = new WeakReferenceOf<ManagedDisposableObject>(obj);
            obj = null;
            DoFullGarbageCollection();
            Assert.AreEqual(id, weakref.Id);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyIsAliveReturnsTrueWhenObjectIsAlive()
        {
            var obj = new ManagedDisposableObject();
            var weakref = new WeakReferenceOf<ManagedDisposableObject>(obj);
            Assert.IsTrue(weakref.IsAlive);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyIsAliveReturnsFalseWhenObjectIsGarbageCollected()
        {
            var weakref = new WeakReferenceOf<ManagedDisposableObject>(new ManagedDisposableObject());
            DoFullGarbageCollection();
            Assert.IsFalse(weakref.IsAlive);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyTargetReturnsObjectWhenObjectIsAlive()
        {
            var obj = new ManagedDisposableObject();
            var weakref = new WeakReferenceOf<ManagedDisposableObject>(obj);
            Assert.AreEqual(obj, weakref.Target);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyTargetReturnsNullWhenObjectIsGarbageCollected()
        {
            var weakref = new WeakReferenceOf<ManagedDisposableObject>(new ManagedDisposableObject());
            DoFullGarbageCollection();
            Assert.AreEqual(null, weakref.Target);
        }

        private static void DoFullGarbageCollection()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }
}