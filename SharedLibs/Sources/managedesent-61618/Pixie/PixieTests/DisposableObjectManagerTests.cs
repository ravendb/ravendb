//-----------------------------------------------------------------------
// <copyright file="DisposableObjectManagerTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the disposable object manager.
    /// </summary>
    [TestClass]
    public class DisposableObjectManagerTests
    {
        private DisposableObjectManager manager;

        [TestInitialize]
        public void Setup()
        {
            this.manager = new DisposableObjectManager();
        }

        [TestCleanup]
        public void Teardown()
        {
            this.manager.Dispose();
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyManagedDisposableObjectConstructorSetsWasDisposedToFalse()
        {
            var object1 = new ManagedDisposableObject();
            Assert.IsFalse(object1.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyManagedDisposableObjectDisposeSetsWasDisposedToTrue()
        {
            var object1 = new ManagedDisposableObject();
            object1.Dispose();
            Assert.IsTrue(object1.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyManagedDisposableObjectDisposeFiresEvent()
        {
            VerifyIManagedDisposableObjectDisposeFiresEvent(new ManagedDisposableObject());
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyManagedDisposableObjectDisposePassesSelfToEvent()
        {
            VerifyIManagedDisposableObjectDisposePassesSelfToEvent(new ManagedDisposableObject());
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectManagerDisposeFiresEvent()
        {
            VerifyIManagedDisposableObjectDisposeFiresEvent(new DisposableObjectManager());
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDisposableObjectManagerDisposePassesSelfToEvent()
        {
            VerifyIManagedDisposableObjectDisposePassesSelfToEvent(new DisposableObjectManager());
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyGetNewDisposableObjectIdReturnsUniqueValues()
        {
            DisposableObjectId id1 = DisposableObjectManager.GetNewDisposableObjectId();
            DisposableObjectId id2 = DisposableObjectManager.GetNewDisposableObjectId();
            Assert.AreNotEqual(id1, id2);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyLinkedListAddExtensionAddsItem()
        {
            var list = new LinkedList<string>();
            list.Add("hello");
            Assert.AreEqual(list.First.Value, "hello");
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChildObjectIsDisposedWhenParentIsDisposed()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);

            parent.Dispose();
            Assert.IsTrue(child.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void TestDisposeTwice()
        {
            var obj = new ManagedDisposableObject();

            this.manager.Register(obj);
            obj.Dispose();
            obj.Dispose();
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyAllChildrenAreDisposedWhenParentIsDisposed()
        {
            var parent = new ManagedDisposableObject();
            var children = new ManagedDisposableObject[] 
            {
                new ManagedDisposableObject(),
                new ManagedDisposableObject(),
                new ManagedDisposableObject(),
                new ManagedDisposableObject(),
                new ManagedDisposableObject(),
                new ManagedDisposableObject(),
            };

            this.manager.Register(parent);
            foreach (ManagedDisposableObject child in children)
            {
                this.manager.RegisterAsDependent(child, parent);
            }

            parent.Dispose();
            foreach (ManagedDisposableObject child in children)
            {
                Assert.IsTrue(child.WasDisposed);
            }
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyParentIsNotDisposedWhenChildIsDisposed()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);

            child.Dispose();
            Assert.IsFalse(parent.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChildAndGrandchildAreDisposedWhenParentIsDisposed()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();
            var grandchild = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);
            this.manager.RegisterAsDependent(grandchild, child);

            parent.Dispose();
            Assert.IsTrue(child.WasDisposed);
            Assert.IsTrue(grandchild.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyAllObjectAreDisposedWhenManagerIsDisposed()
        {
            var manager = new DisposableObjectManager();

            var parent = new ManagedDisposableObject();
            var child1 = new ManagedDisposableObject();
            var child2 = new ManagedDisposableObject();
            var child3 = new ManagedDisposableObject();
            var grandchild = new ManagedDisposableObject();

            manager.Register(parent);
            manager.RegisterAsDependent(child1, parent);
            manager.RegisterAsDependent(child2, parent);
            manager.RegisterAsDependent(child3, parent);
            manager.RegisterAsDependent(grandchild, child2);

            manager.Dispose();
            Assert.IsTrue(parent.WasDisposed);
            Assert.IsTrue(child1.WasDisposed);
            Assert.IsTrue(child2.WasDisposed);
            Assert.IsTrue(child3.WasDisposed);
            Assert.IsTrue(grandchild.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyGrandchildIsDisposedWhenChildIsDisposed()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();
            var grandchild = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);
            this.manager.RegisterAsDependent(grandchild, child);

            child.Dispose();
            Assert.IsFalse(parent.WasDisposed);
            Assert.IsTrue(grandchild.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChildIsNotDisposedWhenGrandchildIsDisposed()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();
            var grandchild = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);
            this.manager.RegisterAsDependent(grandchild, child);

            grandchild.Dispose();
            Assert.IsFalse(parent.WasDisposed);
            Assert.IsFalse(child.WasDisposed);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyObjectWithDependentsCannotBeGarbageCollected()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);

            var weakRef = new WeakReference(parent);
            parent = null;

            TestUtilities.FinalizeAndGCCollect();
            Assert.IsTrue(weakRef.IsAlive);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyObjectWithNoDependentsCanBeGarbageCollected()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();

            this.manager.Register(parent);
            this.manager.RegisterAsDependent(child, parent);

            var weakRef = new WeakReference(child);
            child = null;

            TestUtilities.FinalizeAndGCCollect();
            Assert.IsFalse(weakRef.IsAlive);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyGarbageCollectionCanGetAllObjects()
        {
            var grandparent = new ManagedDisposableObject();
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();
            var grandchild = new ManagedDisposableObject();

            this.manager.Register(grandparent);
            this.manager.RegisterAsDependent(parent, grandparent);
            this.manager.RegisterAsDependent(child, parent);
            this.manager.RegisterAsDependent(grandchild, child);

            var grandparentRef = new WeakReference(grandparent);
            var parentRef = new WeakReference(parent);
            var childRef = new WeakReference(child);
            var grandchildRef = new WeakReference(grandchild);

            grandparent = parent = child = grandchild = null;

            // Each garbage collection should get at least one level
            // of the dependency tree
            TestUtilities.FinalizeAndGCCollect();
            Assert.IsFalse(grandchildRef.IsAlive, "grandchild is still alive");
            TestUtilities.FinalizeAndGCCollect();
            Assert.IsFalse(childRef.IsAlive, "child is still alive");
            TestUtilities.FinalizeAndGCCollect();
            Assert.IsFalse(parentRef.IsAlive, "parent is still alive");
            TestUtilities.FinalizeAndGCCollect();
            Assert.IsFalse(grandparentRef.IsAlive, "grandparent is still alive");
        }

        [TestMethod]
        [Priority(1)]
        public void TestDisposingParentWhenSomeChildrenAreUnreferenced()
        {
            var parent = new ManagedDisposableObject();
            var child = new ManagedDisposableObject();

            this.manager.Register(parent);

            this.manager.RegisterAsDependent(new ManagedDisposableObject(), parent);
            this.manager.RegisterAsDependent(child, parent);
            this.manager.RegisterAsDependent(new ManagedDisposableObject(), parent);

            TestUtilities.FinalizeAndGCCollect();
            Assert.IsFalse(child.WasDisposed);
            parent.Dispose();
            Assert.IsTrue(child.WasDisposed);
        }

        private static void VerifyIManagedDisposableObjectDisposeFiresEvent(IManagedDisposableObject obj)
        {
            bool eventFired = false;

            obj.Disposed += ignored => eventFired = true;
            obj.Dispose();
            Assert.IsTrue(eventFired);
        }

        private static void VerifyIManagedDisposableObjectDisposePassesSelfToEvent(IManagedDisposableObject obj)
        {
            IManagedDisposableObject arg = null;

            obj.Disposed += x => arg = x;
            obj.Dispose();
            Assert.AreEqual(obj, arg);
        }
    }

    internal class ManagedDisposableObject : IManagedDisposableObject
    {
        public ManagedDisposableObject()
        {
            this.WasDisposed = false;
            this.Id = DisposableObjectManager.GetNewDisposableObjectId();
        }

        // Need a finalizer so that garbage collection works
        ~ManagedDisposableObject()
        {
            this.Dispose();
        }

        public event Action<IManagedDisposableObject> Disposed;

        public bool WasDisposed { get; private set; }

        public DisposableObjectId Id { get; private set; }

        public void Dispose()
        {
            if (null != this.Disposed)
            {
                this.Disposed(this);
            }

            this.WasDisposed = true;
            GC.SuppressFinalize(this);
        }
    }
}