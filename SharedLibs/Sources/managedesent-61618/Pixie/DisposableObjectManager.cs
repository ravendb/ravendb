//-----------------------------------------------------------------------
// <copyright file="DisposableObjectManager.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Microsoft.Isam
{
    // A WeakReferenceCollection stores a set of weak references to IManagedDisposableObjects. It
    // is used to track which objects must be closed when an object is disposed.
    // This collection can be a HashSet, List or LinkedList.
    //
    // An alias (created with 'using') cannot reference another alias in the same namespace. That means
    // creating the WeakReferenceCollection at a higher-level namespace, which is why WeakReferenceCollection
    // is created here.
    using WeakReferenceCollection = System.Collections.Generic.LinkedList<Microsoft.Isam.Esent.WeakReferenceOf<Microsoft.Isam.Esent.IManagedDisposableObject>>;

    namespace Esent
    {
        // A DependentObjectsDictionary maps an object to the WeakReferenceCollection specifying all
        // dependent objects. This collection can be a Dictionary, SortedDictionary or SortedList.
        using DependentObjectsDictionary = System.Collections.Generic.SortedDictionary<DisposableObjectId, WeakReferenceCollection>;

        // A DependentOnDictionary maps an object to the object it depends on. This collection can
        // be a Dictionary, SortedDictionary or SortedList.
        using DependentOnDictionary = System.Collections.Generic.SortedList<DisposableObjectId, IManagedDisposableObject>;

        /// <summary>
        /// This interface must be implemented by any objects that want
        /// to participate in disposable object management. It augments
        /// IDisposable with an ID (used to track dependencies) and an
        /// event (used to know when an object is being disposed).
        /// </summary>
        internal interface IManagedDisposableObject : IDisposable
        {
            /// <summary>
            /// Event that should be signalled BEFORE the object is
            /// closed/disposed.
            /// </summary>
            event Action<IManagedDisposableObject> Disposed;

            /// <summary>
            /// Gets the id of the object. This must be stable and unique to the process.
            /// </summary>
            DisposableObjectId Id { get; }
        }

        /// <summary>
        /// Extension methods for the WeakReferenceCollection. These methods make the code that uses the collection more 
        /// agnostic towards the underlying type.
        /// </summary>
        internal static class WeakReferenceCollectionExtensions
        {
            /// <summary>
            /// Add a new object to the front of a LinkedList.
            /// </summary>
            /// <remarks>
            /// This is needed because List and HashSet support add, but LinkedList doesn't.
            /// If WeakReferenceCollection is defined to be a LinkedList it needs an Add method.
            /// </remarks>
            /// <typeparam name="T">The type of object in the list.</typeparam>
            /// <param name="list">The list to add to.</param>
            /// <param name="obj">The object to add.</param>
            public static void Add<T>(this LinkedList<T> list, T obj)
            {
                list.AddFirst(obj);
            }

            /// <summary>
            /// Remove a reference to the specified object (if there is a live reference).
            /// </summary>
            /// <param name="collection">The collection to remove the reference from.</param>
            /// <param name="id">The id of the object to remove references to.</param>
            public static void RemoveReferenceTo(this WeakReferenceCollection collection, DisposableObjectId id)
            {
                foreach (WeakReferenceOf<IManagedDisposableObject> weakreference in collection)
                {
                    if (weakreference.Id.Equals(id))
                    {
                        // We expect only one reference to the object
                        collection.Remove(weakreference);
                        Debug.Assert(collection.Where(x => x.Id.Equals(id)).Count() == 0, "Expected only one reference");
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// This class provides ordered disposal of objects. An object can register as
        /// being dependent on another object and it will be automatically disposed when
        /// the depended-on object is disposed. This is needed because ESENT objects
        /// must be released in a specific order and should not be orphaned.
        /// </summary>
        internal class DisposableObjectManager : IManagedDisposableObject
        {
            /// <summary>
            /// The root object of the dependency tree has this id.
            /// </summary>
            private static readonly DisposableObjectId rootId = new DisposableObjectId { Value = Int64.MinValue };

            /// <summary>
            /// Managed disposable objects need unique IDs. This is used to allocate them.
            /// </summary>
            private static long id = 1;

            /// <summary>
            /// Maps an object to the list of objects which depend on it. This is used to
            /// find all dependent object so that they can be closed before the parent object
            /// is closed. We want the dependent object to be garbage collected if possible,
            /// so this list just holds a weak reference to the dependent object.
            /// </summary>
            private readonly DependentObjectsDictionary dependencies;

            /// <summary>
            /// Maps an object to the object is depends on. When the child object is Disposed this
            /// is used to find the parent object so that the disposed object can be removed from
            /// the list of dependents. We don't want the depended-on object to be garbage collected
            /// so this dictionary keeps a strong reference, which prevents garbage collection.
            /// </summary>
            private readonly DependentOnDictionary dependentOn;

            /// <summary>
            /// Used to synchronize the object.
            /// </summary>
            private readonly object lockObject;

            /// <summary>
            /// Set to true when this object is finalized.
            /// </summary>
            private bool wasFinalized;

            /// <summary>
            /// Set to true when this object is disposed.
            /// </summary>
            private bool wasDisposed;

            /// <summary>
            /// Initializes a new instance of the DisposableObjectManager class.
            /// </summary>
            public DisposableObjectManager()
            {
                this.lockObject = new object();
                this.dependencies = new DependentObjectsDictionary
                {
                    // Register this object (the root object)
                    { this.Id, new WeakReferenceCollection() }
                };
                this.dependentOn = new DependentOnDictionary();
            }

            /// <summary>
            /// Finalizes an instance of the DisposableObjectManager class.
            /// </summary>
            ~DisposableObjectManager()
            {
                lock (this.lockObject)
                {
                    // Don't actually do anything -- finalizers are run in an arbitrary order
                    // so there is no way to bring order to the chaos.
                    this.wasFinalized = true;
                }
            }

            /// <summary>
            /// Signalled when this object is disposed.
            /// </summary>
            public event Action<IManagedDisposableObject> Disposed;

            /// <summary>
            /// Gets the id of this object. A DisposableObjectManager is always the root, so this
            /// returns the root id.
            /// </summary>
            public DisposableObjectId Id
            {
                get
                {
                    return DisposableObjectManager.rootId;
                }
            }

            /// <summary>
            /// Get a new, unique ID which can be used for an object. 
            /// </summary>
            /// <returns>A new, unique ID.</returns>
            public static DisposableObjectId GetNewDisposableObjectId()
            {
                long objectId = Interlocked.Add(ref DisposableObjectManager.id, 1);
                return new DisposableObjectId() { Value = objectId };
            }

            /// <summary>
            /// Disposes of this object.
            /// </summary>
            public void Dispose()
            {
                lock (this.lockObject)
                {
                    this.wasDisposed = true;
                    this.DisposeDependents(this);
                    if (null != this.Disposed)
                    {
                        this.Disposed(this);
                    }
                }
            }

            /// <summary>
            /// Register a new managed disposable object. This object isn't dependent
            /// on anything, but can be depended on.
            /// </summary>
            /// <param name="obj">The object to register.</param>
            public void Register(IManagedDisposableObject obj)
            {
                lock (this.lockObject)
                {
                    this.CheckObjectIsNotRegistered(obj);
                    this.RegisterAsDependent(obj, this);
                    this.CheckObjectIsRegistered(obj);
                }
            }

            /// <summary>
            /// Register an object as dependent on another object. If the depended-on object is
            /// disposed then the registered object will be disposed too.
            /// </summary>
            /// <param name="child">The object to register.</param>
            /// <param name="parent">
            /// The depended on object. This object must have been registered already.
            /// </param>
            public void RegisterAsDependent(IManagedDisposableObject child, IManagedDisposableObject parent)
            {
                lock (this.lockObject)
                {
                    this.CheckObjectIsNotRegistered(child);
                    this.CheckObjectIsRegistered(parent);

                    // Add the child to the list of the parent's dependents
                    this.dependencies[parent.Id].Add(new WeakReferenceOf<IManagedDisposableObject>(child));

                    // A child starts out with no dependents
                    this.dependencies[child.Id] = new WeakReferenceCollection();

                    // The child is dependent on the parent
                    this.dependentOn[child.Id] = parent;

                    // We need to know when the child is disposed
                    child.Disposed += this.OnObjectDispose;

                    this.CheckObjectIsRegistered(child);
                }
            }

            /// <summary>
            /// Unregister an object. The object must have been registered with
            /// this manager.
            /// </summary>
            /// <param name="obj">The object to remove the registration for.</param>
            private void Unregister(IManagedDisposableObject obj)
            {
                lock (this.lockObject)
                {
                    this.CheckObjectIsRegistered(obj);
                    this.RemoveEntriesFor(obj.Id);
                    this.CheckObjectIsNotRegistered(obj);
                }
            }

            /// <summary>
            /// Called when a managed disposable object is about to be disposed. This methods
            /// disposes all dependent ojects.
            /// </summary>
            /// <param name="obj">The object being disposed.</param>
            private void OnObjectDispose(IManagedDisposableObject obj)
            {
                // Remove the object callback so that this method isn't called twice
                obj.Disposed -= this.OnObjectDispose;

                lock (this.lockObject)
                {
                    // During process shutdown and AppDomain unloading objects in the
                    // RegisteredForFinalization queue are finalized even though they are still
                    // reachable. This means the objects can be finalized in an unexpected
                    // fashion. To deal with that we ignore these callbacks during shutdown
                    // and AppDomain unload.
                    //
                    // An object can be finalized/disposed multiple times. We have to make 
                    // sure this object is still registered.
                    if (!AppDomain.CurrentDomain.IsFinalizingForUnload()
                        && !Environment.HasShutdownStarted
                        && !this.wasDisposed
                        && !this.wasFinalized
                        && this.dependencies.ContainsKey(obj.Id))
                    {
                        this.DisposeDependents(obj);
                        this.Unregister(obj);
                    }
                }
            }

            /// <summary>
            /// Remove all dependencies/dependentOn entries for the given id.
            /// </summary>
            /// <param name="objectId">The id to remove entries for.</param>
            private void RemoveEntriesFor(DisposableObjectId objectId)
            {
                // Remove the object from the list of its parent's dependencies
                IManagedDisposableObject parent = this.dependentOn[objectId];
                this.dependencies[parent.Id].RemoveReferenceTo(objectId);
                this.dependentOn.Remove(objectId);

                // Delete the list of dependencies for this object
                this.dependencies.Remove(objectId);
            }

            /// <summary>
            /// Call the dispose method of all dependent objects.
            /// </summary>
            /// <param name="obj">
            /// The depended on object. This object will not be disposed, but its
            /// dependents will be.
            /// </param>
            private void DisposeDependents(IManagedDisposableObject obj)
            {
                // Disposing an object removes it from the list. To avoid trying to iterate a list
                // that is being modified, we copy the list first.
                var referencesToDispose = new WeakReferenceCollection(this.dependencies[obj.Id]);
                foreach (WeakReferenceOf<IManagedDisposableObject> reference in referencesToDispose)
                {
                    IManagedDisposableObject objectToDispose = reference.Target;
                    if (null != objectToDispose)
                    {
                        this.DisposeObject(objectToDispose);
                    }
                    else
                    {
                        // The weak reference can't be resolved. Remove all entries.
                        this.dependencies[obj.Id].RemoveReferenceTo(reference.Id);
                        this.RemoveEntriesFor(reference.Id);
                    }
                }
            }

            /// <summary>
            /// Call the dispose method of the specified object.
            /// </summary>
            /// <param name="obj">The object to dispose.</param>
            private void DisposeObject(IManagedDisposableObject obj)
            {
                // Recursively dispose all dependent objects, deregister
                // (to avoid the Disposed event) then dispose.
                this.DisposeDependents(obj);
                this.Unregister(obj);
                obj.Dispose();
            }

            /// <summary>
            /// Throw an exception if the given object isn't registered.
            /// </summary>
            /// <param name="obj">The object to look for.</param>
            private void CheckObjectIsRegistered(IManagedDisposableObject obj)
            {
                if (!this.dependencies.ContainsKey(obj.Id))
                {
                    throw new InvalidOperationException("Object is not registered");
                }
            }

            /// <summary>
            /// Throw an exception if the given object isn't already registered.
            /// </summary>
            /// <param name="obj">The object to look for.</param>
            private void CheckObjectIsNotRegistered(IManagedDisposableObject obj)
            {
                if (this.dependencies.ContainsKey(obj.Id))
                {
                    throw new InvalidOperationException("Object is already registered");
                }
            }
        }
    }
}