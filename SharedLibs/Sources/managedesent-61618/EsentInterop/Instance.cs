//-----------------------------------------------------------------------
// <copyright file="Instance.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Runtime.CompilerServices;
    using System.Security.Permissions;

    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// A class that encapsulates a <see cref="JET_INSTANCE"/> in a disposable object. The
    /// instance must be closed last and closing the instance releases all other
    /// resources for the instance.
    /// </summary>
    public class Instance : SafeHandleZeroOrMinusOneIsInvalid
    {
        /// <summary>
        /// Parameters for the instance.
        /// </summary>
        private readonly InstanceParameters parameters;

        /// <summary>
        /// The name of the instance.
        /// </summary>
        private readonly string name;

        /// <summary>
        /// The display name of the instance.
        /// </summary>
        private readonly string displayName;

        /// <summary>
        /// Initializes a new instance of the Instance class. The underlying
        /// JET_INSTANCE is allocated, but not initialized.
        /// </summary>
        /// <param name="name">
        /// The name of the instance. This string must be unique within a
        /// given process hosting the database engine.
        /// </param>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public Instance(string name) : this(name, name)
        {
        }

        /// <summary>
        /// Initializes a new instance of the Instance class. The underlying
        /// JET_INSTANCE is allocated, but not initialized.
        /// </summary>
        /// <param name="name">
        /// The name of the instance. This string must be unique within a
        /// given process hosting the database engine.
        /// </param>
        /// <param name="displayName">
        /// A display name for the instance. This will be used in eventlog
        /// entries.
        /// </param>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public Instance(string name, string displayName) : base(true)
        {
            this.name = name;
            this.displayName = displayName;

            JET_INSTANCE instance;
            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                this.SetHandle(JET_INSTANCE.Nil.Value);
            }
            finally
            {
                // This is the code that we want in a constrained execution region.
                // We need to avoid the situation where JetCreateInstance2 is called
                // but the handle isn't set, so the instance is never terminated.
                // This would happen, for example, if there was a ThreadAbortException
                // between the call to JetCreateInstance2 and the call to SetHandle.
                //
                // If an Esent exception is generated we do not want to call SetHandle
                // because the instance isn't valid. On the other hand if a different 
                // exception (out of memory or thread abort) is generated we still need
                // to set the handle to avoid losing track of the instance. The call to
                // JetCreateInstance2 is in the CER to make sure that the only exceptions
                // which can be generated are from ESENT failures.
                Api.JetCreateInstance2(out instance, this.name, this.displayName, CreateInstanceGrbit.None);
                this.SetHandle(instance.Value);
            }

            this.parameters = new InstanceParameters(instance);
        }

        /// <summary>
        /// Gets the JET_INSTANCE that this instance contains.
        /// </summary>
        public JET_INSTANCE JetInstance
        {
            [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
            get
            {
                this.CheckObjectIsNotDisposed();
                return this.CreateInstanceFromHandle();
            }
        }

        /// <summary>
        /// Gets the InstanceParameters for this instance. 
        /// </summary>
        public InstanceParameters Parameters
        {
            [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
            get
            {
                this.CheckObjectIsNotDisposed();
                return this.parameters;
            }
        }

        /// <summary>
        /// Provide implicit conversion of an Instance object to a JET_INSTANCE
        /// structure. This is done so that an Instance can be used anywhere a
        /// JET_INSTANCE is required.
        /// </summary>
        /// <param name="instance">The instance to convert.</param>
        /// <returns>The JET_INSTANCE wrapped by the instance.</returns>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public static implicit operator JET_INSTANCE(Instance instance)
        {
            return instance.JetInstance;
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="Instance"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="Instance"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "{0} ({1})", this.displayName, this.name);
        }

        /// <summary>
        /// Initialize the JET_INSTANCE.
        /// </summary>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public void Init()
        {
            this.Init(InitGrbit.None);
        }

        /// <summary>
        /// Initialize the JET_INSTANCE.
        /// </summary>
        /// <param name="grbit">
        /// Initialization options.
        /// </param>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public void Init(InitGrbit grbit)
        {
            this.CheckObjectIsNotDisposed();
            JET_INSTANCE instance = this.JetInstance;

            // Use a constrained region so that the handle is
            // always set after JetInit2 is called.
            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                // Remember that a failure in JetInit can zero the handle
                // and that JetTerm should not be called in that case.
                Api.JetInit2(ref instance, grbit);
            }
            finally
            {
                this.SetHandle(instance.Value);
            }
        }

        /// <summary>
        /// Initialize the JET_INSTANCE. This API requires at least the
        /// Vista version of ESENT.
        /// </summary>
        /// <param name="recoveryOptions">
        /// Additional recovery parameters for remapping databases during
        /// recovery, position where to stop recovery at, or recovery status.
        /// </param>
        /// <param name="grbit">
        /// Initialization options.
        /// </param>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public void Init(JET_RSTINFO recoveryOptions, InitGrbit grbit)
        {
            this.CheckObjectIsNotDisposed();
            JET_INSTANCE instance = this.JetInstance;

            // Use a constrained region so that the handle is
            // always set after JetInit3 is called.
            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                // Remember that a failure in JetInit can zero the handle
                // and that JetTerm should not be called in that case.
                VistaApi.JetInit3(ref instance, recoveryOptions, grbit);
            }
            finally
            {
                this.SetHandle(instance.Value);
            }
        }

        /// <summary>
        /// Terminate the JET_INSTANCE.
        /// </summary>
        [SuppressMessage(
            "Microsoft.StyleCop.CSharp.MaintainabilityRules",
            "SA1409:RemoveUnnecessaryCode",
            Justification = "CER code belongs in the finally block, so the try clause is empty")]
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public void Term()
        {
            // Use a constrained region so that the handle is
            // always set as invalid after JetTerm is called.
            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                // This try block deliberately left blank.
            }
            finally
            {
                // This is the code that we want in a constrained execution region.
                // We need to avoid the situation where JetTerm is called
                // but the handle isn't invalidated, so the instance is terminated again.
                // This would happen, for example, if there was a ThreadAbortException
                // between the call to JetTerm and the call to SetHandle.
                //
                // If an Esent exception is generated we do not want to invalidate the handle
                // because the instance isn't necessarily terminated. On the other hand if a 
                // different exception (out of memory or thread abort) is generated we still need
                // to invalidate the handle.
                Api.JetTerm(this.JetInstance);
                this.SetHandleAsInvalid();                
            }
        }

        /// <summary>
        /// Release the handle for this instance.
        /// </summary>
        /// <returns>True if the handle could be released.</returns>
        protected override bool ReleaseHandle()
        {
            // The object is already marked as invalid so don't check
            var instance = this.CreateInstanceFromHandle();
            return (int)JET_err.Success == Api.Impl.JetTerm(instance);
        }

        /// <summary>
        /// Create a JET_INSTANCE from the internal handle value.
        /// </summary>
        /// <returns>A JET_INSTANCE containing the internal handle.</returns>
        private JET_INSTANCE CreateInstanceFromHandle()
        {
            return new JET_INSTANCE { Value = this.handle };
        }

        /// <summary>
        /// Check to see if this instance is invalid or closed.
        /// </summary>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        private void CheckObjectIsNotDisposed()
        {
            if (this.IsInvalid || this.IsClosed)
            {
                throw new ObjectDisposedException("Instance");
            }
        }
    }
}