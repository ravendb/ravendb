//-----------------------------------------------------------------------
// <copyright file="ErrorExceptions.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

// Auto generated

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.Serialization;

    // The basic exception hierarchy ...
    //
    // EsentErrorException
    //    |
    //    |-- EsentOperationException
    //    |     |-- EsentFatalException
    //    |     |-- EsentIOException               // bad IO issues, may or may not be transient.
    //    |     |-- EsentResourceException
    //    |           |-- EsentMemoryException    // out of memory (all variants)
    //    |           |-- EsentQuotaException    
    //    |           |-- EsentDiskException    // out of disk space (all variants)
    //    |-- EsentDataException
    //    |     |-- EsentCorruptionException
    //    |     |-- EsentInconsistentException
    //    |     |-- EsentFragmentationException
    //    |-- EsentApiException
    //          |-- EsentUsageException
    //          |-- EsentStateException
    //          |-- EsentObsoleteException

    /// <summary>
    /// Base class for Operation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentOperationException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOperationException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentOperationException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOperationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentOperationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Data exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentDataException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDataException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentDataException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDataException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentDataException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Api exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentApiException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentApiException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentApiException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentApiException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentApiException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Fatal exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentFatalException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFatalException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentFatalException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFatalException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentFatalException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for IO exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentIOException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIOException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentIOException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIOException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentIOException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Resource exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentResourceException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentResourceException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentResourceException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentResourceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentResourceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Memory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentMemoryException : EsentResourceException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMemoryException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentMemoryException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMemoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentMemoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Quota exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentQuotaException : EsentResourceException
    {
        /// <summary>
        /// Initializes a new instance of the EsentQuotaException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentQuotaException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentQuotaException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentQuotaException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Disk exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentDiskException : EsentResourceException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDiskException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentDiskException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDiskException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentDiskException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Corruption exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentCorruptionException : EsentDataException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCorruptionException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentCorruptionException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCorruptionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentCorruptionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Inconsistent exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentInconsistentException : EsentDataException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInconsistentException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentInconsistentException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInconsistentException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentInconsistentException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Fragmentation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentFragmentationException : EsentDataException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFragmentationException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentFragmentationException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFragmentationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentFragmentationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Usage exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentUsageException : EsentApiException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUsageException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentUsageException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUsageException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentUsageException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for State exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentStateException : EsentApiException
    {
        /// <summary>
        /// Initializes a new instance of the EsentStateException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentStateException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentStateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentStateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for Obsolete exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public abstract class EsentObsoleteException : EsentApiException
    {
        /// <summary>
        /// Initializes a new instance of the EsentObsoleteException class.
        /// </summary>
        /// <param name="description">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        protected EsentObsoleteException(string description, JET_err err) :
            base(description, err)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentObsoleteException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentObsoleteException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RfsFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRfsFailureException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRfsFailureException class.
        /// </summary>
        public EsentRfsFailureException() :
            base("Resource Failure Simulator failure", JET_err.RfsFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRfsFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRfsFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RfsNotArmed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRfsNotArmedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRfsNotArmedException class.
        /// </summary>
        public EsentRfsNotArmedException() :
            base("Resource Failure Simulator not initialized", JET_err.RfsNotArmed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRfsNotArmedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRfsNotArmedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileClose exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileCloseException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileCloseException class.
        /// </summary>
        public EsentFileCloseException() :
            base("Could not close file", JET_err.FileClose)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileCloseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileCloseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfThreads exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfThreadsException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfThreadsException class.
        /// </summary>
        public EsentOutOfThreadsException() :
            base("Could not start thread", JET_err.OutOfThreads)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfThreadsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfThreadsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyIO exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyIOException : EsentResourceException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyIOException class.
        /// </summary>
        public EsentTooManyIOException() :
            base("System busy due to too many IOs", JET_err.TooManyIO)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyIOException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyIOException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TaskDropped exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTaskDroppedException : EsentResourceException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTaskDroppedException class.
        /// </summary>
        public EsentTaskDroppedException() :
            base("A requested async task could not be executed", JET_err.TaskDropped)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTaskDroppedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTaskDroppedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InternalError exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInternalErrorException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInternalErrorException class.
        /// </summary>
        public EsentInternalErrorException() :
            base("Fatal internal error", JET_err.InternalError)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInternalErrorException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInternalErrorException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DisabledFunctionality exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDisabledFunctionalityException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDisabledFunctionalityException class.
        /// </summary>
        public EsentDisabledFunctionalityException() :
            base("You are running MinESE, that does not have all features compiled in.  This functionality is only supported in a full version of ESE.", JET_err.DisabledFunctionality)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDisabledFunctionalityException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDisabledFunctionalityException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseBufferDependenciesCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseBufferDependenciesCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseBufferDependenciesCorruptedException class.
        /// </summary>
        public EsentDatabaseBufferDependenciesCorruptedException() :
            base("Buffer dependencies improperly set. Recovery failure", JET_err.DatabaseBufferDependenciesCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseBufferDependenciesCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseBufferDependenciesCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PreviousVersion exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPreviousVersionException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPreviousVersionException class.
        /// </summary>
        public EsentPreviousVersionException() :
            base("Version already existed. Recovery failure", JET_err.PreviousVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPreviousVersionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPreviousVersionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PageBoundary exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPageBoundaryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPageBoundaryException class.
        /// </summary>
        public EsentPageBoundaryException() :
            base("Reached Page Boundary", JET_err.PageBoundary)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPageBoundaryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPageBoundaryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.KeyBoundary exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentKeyBoundaryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentKeyBoundaryException class.
        /// </summary>
        public EsentKeyBoundaryException() :
            base("Reached Key Boundary", JET_err.KeyBoundary)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentKeyBoundaryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentKeyBoundaryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadPageLink exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadPageLinkException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadPageLinkException class.
        /// </summary>
        public EsentBadPageLinkException() :
            base("Database corrupted", JET_err.BadPageLink)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadPageLinkException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadPageLinkException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadBookmark exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadBookmarkException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadBookmarkException class.
        /// </summary>
        public EsentBadBookmarkException() :
            base("Bookmark has no corresponding address in database", JET_err.BadBookmark)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadBookmarkException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadBookmarkException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NTSystemCallFailed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNTSystemCallFailedException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNTSystemCallFailedException class.
        /// </summary>
        public EsentNTSystemCallFailedException() :
            base("A call to the operating system failed", JET_err.NTSystemCallFailed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNTSystemCallFailedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNTSystemCallFailedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadParentPageLink exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadParentPageLinkException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadParentPageLinkException class.
        /// </summary>
        public EsentBadParentPageLinkException() :
            base("Database corrupted", JET_err.BadParentPageLink)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadParentPageLinkException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadParentPageLinkException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SPAvailExtCacheOutOfSync exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSPAvailExtCacheOutOfSyncException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSPAvailExtCacheOutOfSyncException class.
        /// </summary>
        public EsentSPAvailExtCacheOutOfSyncException() :
            base("AvailExt cache doesn't match btree", JET_err.SPAvailExtCacheOutOfSync)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSPAvailExtCacheOutOfSyncException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSPAvailExtCacheOutOfSyncException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SPAvailExtCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSPAvailExtCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSPAvailExtCorruptedException class.
        /// </summary>
        public EsentSPAvailExtCorruptedException() :
            base("AvailExt space tree is corrupt", JET_err.SPAvailExtCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSPAvailExtCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSPAvailExtCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SPAvailExtCacheOutOfMemory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSPAvailExtCacheOutOfMemoryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSPAvailExtCacheOutOfMemoryException class.
        /// </summary>
        public EsentSPAvailExtCacheOutOfMemoryException() :
            base("Out of memory allocating an AvailExt cache node", JET_err.SPAvailExtCacheOutOfMemory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSPAvailExtCacheOutOfMemoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSPAvailExtCacheOutOfMemoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SPOwnExtCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSPOwnExtCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSPOwnExtCorruptedException class.
        /// </summary>
        public EsentSPOwnExtCorruptedException() :
            base("OwnExt space tree is corrupt", JET_err.SPOwnExtCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSPOwnExtCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSPOwnExtCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DbTimeCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDbTimeCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDbTimeCorruptedException class.
        /// </summary>
        public EsentDbTimeCorruptedException() :
            base("Dbtime on current page is greater than global database dbtime", JET_err.DbTimeCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDbTimeCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDbTimeCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.KeyTruncated exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentKeyTruncatedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentKeyTruncatedException class.
        /// </summary>
        public EsentKeyTruncatedException() :
            base("key truncated on index that disallows key truncation", JET_err.KeyTruncated)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentKeyTruncatedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentKeyTruncatedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseLeakInSpace exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseLeakInSpaceException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseLeakInSpaceException class.
        /// </summary>
        public EsentDatabaseLeakInSpaceException() :
            base("Some database pages have become unreachable even from the avail tree, only an offline defragmentation can return the lost space.", JET_err.DatabaseLeakInSpace)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseLeakInSpaceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseLeakInSpaceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.KeyTooBig exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentKeyTooBigException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentKeyTooBigException class.
        /// </summary>
        public EsentKeyTooBigException() :
            base("Key is too large", JET_err.KeyTooBig)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentKeyTooBigException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentKeyTooBigException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotSeparateIntrinsicLV exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotSeparateIntrinsicLVException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotSeparateIntrinsicLVException class.
        /// </summary>
        public EsentCannotSeparateIntrinsicLVException() :
            base("illegal attempt to separate an LV which must be intrinsic", JET_err.CannotSeparateIntrinsicLV)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotSeparateIntrinsicLVException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotSeparateIntrinsicLVException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SeparatedLongValue exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSeparatedLongValueException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSeparatedLongValueException class.
        /// </summary>
        public EsentSeparatedLongValueException() :
            base("Operation not supported on separated long-value", JET_err.SeparatedLongValue)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSeparatedLongValueException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSeparatedLongValueException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidLoggedOperation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidLoggedOperationException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidLoggedOperationException class.
        /// </summary>
        public EsentInvalidLoggedOperationException() :
            base("Logged operation cannot be redone", JET_err.InvalidLoggedOperation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidLoggedOperationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidLoggedOperationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogFileCorrupt exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogFileCorruptException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogFileCorruptException class.
        /// </summary>
        public EsentLogFileCorruptException() :
            base("Log file is corrupt", JET_err.LogFileCorrupt)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogFileCorruptException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogFileCorruptException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NoBackupDirectory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNoBackupDirectoryException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNoBackupDirectoryException class.
        /// </summary>
        public EsentNoBackupDirectoryException() :
            base("No backup directory given", JET_err.NoBackupDirectory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNoBackupDirectoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNoBackupDirectoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BackupDirectoryNotEmpty exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBackupDirectoryNotEmptyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBackupDirectoryNotEmptyException class.
        /// </summary>
        public EsentBackupDirectoryNotEmptyException() :
            base("The backup directory is not emtpy", JET_err.BackupDirectoryNotEmpty)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBackupDirectoryNotEmptyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBackupDirectoryNotEmptyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BackupInProgress exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBackupInProgressException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBackupInProgressException class.
        /// </summary>
        public EsentBackupInProgressException() :
            base("Backup is active already", JET_err.BackupInProgress)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBackupInProgressException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBackupInProgressException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RestoreInProgress exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRestoreInProgressException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRestoreInProgressException class.
        /// </summary>
        public EsentRestoreInProgressException() :
            base("Restore in progress", JET_err.RestoreInProgress)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRestoreInProgressException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRestoreInProgressException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingPreviousLogFile exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingPreviousLogFileException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingPreviousLogFileException class.
        /// </summary>
        public EsentMissingPreviousLogFileException() :
            base("Missing the log file for check point", JET_err.MissingPreviousLogFile)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingPreviousLogFileException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingPreviousLogFileException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogWriteFail exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogWriteFailException : EsentIOException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogWriteFailException class.
        /// </summary>
        public EsentLogWriteFailException() :
            base("Failure writing to log file", JET_err.LogWriteFail)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogWriteFailException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogWriteFailException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogDisabledDueToRecoveryFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogDisabledDueToRecoveryFailureException : EsentFatalException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogDisabledDueToRecoveryFailureException class.
        /// </summary>
        public EsentLogDisabledDueToRecoveryFailureException() :
            base("Try to log something after recovery faild", JET_err.LogDisabledDueToRecoveryFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogDisabledDueToRecoveryFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogDisabledDueToRecoveryFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotLogDuringRecoveryRedo exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotLogDuringRecoveryRedoException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotLogDuringRecoveryRedoException class.
        /// </summary>
        public EsentCannotLogDuringRecoveryRedoException() :
            base("Try to log something during recovery redo", JET_err.CannotLogDuringRecoveryRedo)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotLogDuringRecoveryRedoException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotLogDuringRecoveryRedoException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogGenerationMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogGenerationMismatchException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogGenerationMismatchException class.
        /// </summary>
        public EsentLogGenerationMismatchException() :
            base("Name of logfile does not match internal generation number", JET_err.LogGenerationMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogGenerationMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogGenerationMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadLogVersion exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadLogVersionException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadLogVersionException class.
        /// </summary>
        public EsentBadLogVersionException() :
            base("Version of log file is not compatible with Jet version", JET_err.BadLogVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadLogVersionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadLogVersionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidLogSequence exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidLogSequenceException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidLogSequenceException class.
        /// </summary>
        public EsentInvalidLogSequenceException() :
            base("Timestamp in next log does not match expected", JET_err.InvalidLogSequence)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidLogSequenceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidLogSequenceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LoggingDisabled exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLoggingDisabledException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLoggingDisabledException class.
        /// </summary>
        public EsentLoggingDisabledException() :
            base("Log is not active", JET_err.LoggingDisabled)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLoggingDisabledException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLoggingDisabledException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogBufferTooSmall exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogBufferTooSmallException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogBufferTooSmallException class.
        /// </summary>
        public EsentLogBufferTooSmallException() :
            base("Log buffer is too small for recovery", JET_err.LogBufferTooSmall)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogBufferTooSmallException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogBufferTooSmallException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogSequenceEnd exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogSequenceEndException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogSequenceEndException class.
        /// </summary>
        public EsentLogSequenceEndException() :
            base("Maximum log file number exceeded", JET_err.LogSequenceEnd)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogSequenceEndException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogSequenceEndException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NoBackup exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNoBackupException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNoBackupException class.
        /// </summary>
        public EsentNoBackupException() :
            base("No backup in progress", JET_err.NoBackup)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNoBackupException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNoBackupException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidBackupSequence exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidBackupSequenceException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidBackupSequenceException class.
        /// </summary>
        public EsentInvalidBackupSequenceException() :
            base("Backup call out of sequence", JET_err.InvalidBackupSequence)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidBackupSequenceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidBackupSequenceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BackupNotAllowedYet exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBackupNotAllowedYetException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBackupNotAllowedYetException class.
        /// </summary>
        public EsentBackupNotAllowedYetException() :
            base("Cannot do backup now", JET_err.BackupNotAllowedYet)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBackupNotAllowedYetException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBackupNotAllowedYetException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DeleteBackupFileFail exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDeleteBackupFileFailException : EsentIOException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDeleteBackupFileFailException class.
        /// </summary>
        public EsentDeleteBackupFileFailException() :
            base("Could not delete backup file", JET_err.DeleteBackupFileFail)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDeleteBackupFileFailException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDeleteBackupFileFailException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MakeBackupDirectoryFail exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMakeBackupDirectoryFailException : EsentIOException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMakeBackupDirectoryFailException class.
        /// </summary>
        public EsentMakeBackupDirectoryFailException() :
            base("Could not make backup temp directory", JET_err.MakeBackupDirectoryFail)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMakeBackupDirectoryFailException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMakeBackupDirectoryFailException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidBackup exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidBackupException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidBackupException class.
        /// </summary>
        public EsentInvalidBackupException() :
            base("Cannot perform incremental backup when circular logging enabled", JET_err.InvalidBackup)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidBackupException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidBackupException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecoveredWithErrors exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecoveredWithErrorsException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecoveredWithErrorsException class.
        /// </summary>
        public EsentRecoveredWithErrorsException() :
            base("Restored with errors", JET_err.RecoveredWithErrors)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecoveredWithErrorsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecoveredWithErrorsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingLogFile exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingLogFileException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingLogFileException class.
        /// </summary>
        public EsentMissingLogFileException() :
            base("Current log file missing", JET_err.MissingLogFile)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingLogFileException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingLogFileException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogDiskFull exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogDiskFullException : EsentDiskException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogDiskFullException class.
        /// </summary>
        public EsentLogDiskFullException() :
            base("Log disk full", JET_err.LogDiskFull)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogDiskFullException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogDiskFullException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadLogSignature exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadLogSignatureException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadLogSignatureException class.
        /// </summary>
        public EsentBadLogSignatureException() :
            base("Bad signature for a log file", JET_err.BadLogSignature)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadLogSignatureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadLogSignatureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadDbSignature exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadDbSignatureException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadDbSignatureException class.
        /// </summary>
        public EsentBadDbSignatureException() :
            base("Bad signature for a db file", JET_err.BadDbSignature)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadDbSignatureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadDbSignatureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadCheckpointSignature exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadCheckpointSignatureException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadCheckpointSignatureException class.
        /// </summary>
        public EsentBadCheckpointSignatureException() :
            base("Bad signature for a checkpoint file", JET_err.BadCheckpointSignature)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadCheckpointSignatureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadCheckpointSignatureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CheckpointCorrupt exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCheckpointCorruptException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCheckpointCorruptException class.
        /// </summary>
        public EsentCheckpointCorruptException() :
            base("Checkpoint file not found or corrupt", JET_err.CheckpointCorrupt)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCheckpointCorruptException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCheckpointCorruptException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingPatchPage exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingPatchPageException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingPatchPageException class.
        /// </summary>
        public EsentMissingPatchPageException() :
            base("Patch file page not found during recovery", JET_err.MissingPatchPage)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingPatchPageException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingPatchPageException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadPatchPage exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadPatchPageException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadPatchPageException class.
        /// </summary>
        public EsentBadPatchPageException() :
            base("Patch file page is not valid", JET_err.BadPatchPage)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadPatchPageException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadPatchPageException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RedoAbruptEnded exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRedoAbruptEndedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRedoAbruptEndedException class.
        /// </summary>
        public EsentRedoAbruptEndedException() :
            base("Redo abruptly ended due to sudden failure in reading logs from log file", JET_err.RedoAbruptEnded)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRedoAbruptEndedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRedoAbruptEndedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadSLVSignature exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadSLVSignatureException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadSLVSignatureException class.
        /// </summary>
        public EsentBadSLVSignatureException() :
            base("Signature in SLV file does not agree with database", JET_err.BadSLVSignature)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadSLVSignatureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadSLVSignatureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PatchFileMissing exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPatchFileMissingException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPatchFileMissingException class.
        /// </summary>
        public EsentPatchFileMissingException() :
            base("Hard restore detected that patch file is missing from backup set", JET_err.PatchFileMissing)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPatchFileMissingException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPatchFileMissingException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseLogSetMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseLogSetMismatchException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseLogSetMismatchException class.
        /// </summary>
        public EsentDatabaseLogSetMismatchException() :
            base("Database does not belong with the current set of log files", JET_err.DatabaseLogSetMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseLogSetMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseLogSetMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseStreamingFileMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseStreamingFileMismatchException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseStreamingFileMismatchException class.
        /// </summary>
        public EsentDatabaseStreamingFileMismatchException() :
            base("Database and streaming file do not match each other", JET_err.DatabaseStreamingFileMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseStreamingFileMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseStreamingFileMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogFileSizeMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogFileSizeMismatchException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogFileSizeMismatchException class.
        /// </summary>
        public EsentLogFileSizeMismatchException() :
            base("actual log file size does not match JET_paramLogFileSize", JET_err.LogFileSizeMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogFileSizeMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogFileSizeMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CheckpointFileNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCheckpointFileNotFoundException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCheckpointFileNotFoundException class.
        /// </summary>
        public EsentCheckpointFileNotFoundException() :
            base("Could not locate checkpoint file", JET_err.CheckpointFileNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCheckpointFileNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCheckpointFileNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RequiredLogFilesMissing exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRequiredLogFilesMissingException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRequiredLogFilesMissingException class.
        /// </summary>
        public EsentRequiredLogFilesMissingException() :
            base("The required log files for recovery is missing.", JET_err.RequiredLogFilesMissing)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRequiredLogFilesMissingException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRequiredLogFilesMissingException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SoftRecoveryOnBackupDatabase exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSoftRecoveryOnBackupDatabaseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSoftRecoveryOnBackupDatabaseException class.
        /// </summary>
        public EsentSoftRecoveryOnBackupDatabaseException() :
            base("Soft recovery is intended on a backup database. Restore should be used instead", JET_err.SoftRecoveryOnBackupDatabase)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSoftRecoveryOnBackupDatabaseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSoftRecoveryOnBackupDatabaseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogFileSizeMismatchDatabasesConsistent exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogFileSizeMismatchDatabasesConsistentException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogFileSizeMismatchDatabasesConsistentException class.
        /// </summary>
        public EsentLogFileSizeMismatchDatabasesConsistentException() :
            base("databases have been recovered, but the log file size used during recovery does not match JET_paramLogFileSize", JET_err.LogFileSizeMismatchDatabasesConsistent)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogFileSizeMismatchDatabasesConsistentException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogFileSizeMismatchDatabasesConsistentException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogSectorSizeMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogSectorSizeMismatchException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogSectorSizeMismatchException class.
        /// </summary>
        public EsentLogSectorSizeMismatchException() :
            base("the log file sector size does not match the current volume's sector size", JET_err.LogSectorSizeMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogSectorSizeMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogSectorSizeMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogSectorSizeMismatchDatabasesConsistent exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogSectorSizeMismatchDatabasesConsistentException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogSectorSizeMismatchDatabasesConsistentException class.
        /// </summary>
        public EsentLogSectorSizeMismatchDatabasesConsistentException() :
            base("databases have been recovered, but the log file sector size (used during recovery) does not match the current volume's sector size", JET_err.LogSectorSizeMismatchDatabasesConsistent)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogSectorSizeMismatchDatabasesConsistentException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogSectorSizeMismatchDatabasesConsistentException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogSequenceEndDatabasesConsistent exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogSequenceEndDatabasesConsistentException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogSequenceEndDatabasesConsistentException class.
        /// </summary>
        public EsentLogSequenceEndDatabasesConsistentException() :
            base("databases have been recovered, but all possible log generations in the current sequence are used; delete all log files and the checkpoint file and backup the databases before continuing", JET_err.LogSequenceEndDatabasesConsistent)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogSequenceEndDatabasesConsistentException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogSequenceEndDatabasesConsistentException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.StreamingDataNotLogged exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentStreamingDataNotLoggedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentStreamingDataNotLoggedException class.
        /// </summary>
        public EsentStreamingDataNotLoggedException() :
            base("Illegal attempt to replay a streaming file operation where the data wasn't logged. Probably caused by an attempt to roll-forward with circular logging enabled", JET_err.StreamingDataNotLogged)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentStreamingDataNotLoggedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentStreamingDataNotLoggedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseDirtyShutdown exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseDirtyShutdownException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseDirtyShutdownException class.
        /// </summary>
        public EsentDatabaseDirtyShutdownException() :
            base("Database was not shutdown cleanly. Recovery must first be run to properly complete database operations for the previous shutdown.", JET_err.DatabaseDirtyShutdown)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseDirtyShutdownException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseDirtyShutdownException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ConsistentTimeMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentConsistentTimeMismatchException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentConsistentTimeMismatchException class.
        /// </summary>
        public EsentConsistentTimeMismatchException() :
            base("Database last consistent time unmatched", JET_err.ConsistentTimeMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentConsistentTimeMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentConsistentTimeMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabasePatchFileMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabasePatchFileMismatchException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabasePatchFileMismatchException class.
        /// </summary>
        public EsentDatabasePatchFileMismatchException() :
            base("Patch file is not generated from this backup", JET_err.DatabasePatchFileMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabasePatchFileMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabasePatchFileMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.EndingRestoreLogTooLow exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentEndingRestoreLogTooLowException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentEndingRestoreLogTooLowException class.
        /// </summary>
        public EsentEndingRestoreLogTooLowException() :
            base("The starting log number too low for the restore", JET_err.EndingRestoreLogTooLow)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentEndingRestoreLogTooLowException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentEndingRestoreLogTooLowException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.StartingRestoreLogTooHigh exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentStartingRestoreLogTooHighException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentStartingRestoreLogTooHighException class.
        /// </summary>
        public EsentStartingRestoreLogTooHighException() :
            base("The starting log number too high for the restore", JET_err.StartingRestoreLogTooHigh)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentStartingRestoreLogTooHighException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentStartingRestoreLogTooHighException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.GivenLogFileHasBadSignature exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentGivenLogFileHasBadSignatureException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentGivenLogFileHasBadSignatureException class.
        /// </summary>
        public EsentGivenLogFileHasBadSignatureException() :
            base("Restore log file has bad signature", JET_err.GivenLogFileHasBadSignature)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentGivenLogFileHasBadSignatureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentGivenLogFileHasBadSignatureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.GivenLogFileIsNotContiguous exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentGivenLogFileIsNotContiguousException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentGivenLogFileIsNotContiguousException class.
        /// </summary>
        public EsentGivenLogFileIsNotContiguousException() :
            base("Restore log file is not contiguous", JET_err.GivenLogFileIsNotContiguous)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentGivenLogFileIsNotContiguousException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentGivenLogFileIsNotContiguousException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingRestoreLogFiles exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingRestoreLogFilesException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingRestoreLogFilesException class.
        /// </summary>
        public EsentMissingRestoreLogFilesException() :
            base("Some restore log files are missing", JET_err.MissingRestoreLogFiles)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingRestoreLogFilesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingRestoreLogFilesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingFullBackup exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingFullBackupException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingFullBackupException class.
        /// </summary>
        public EsentMissingFullBackupException() :
            base("The database missed a previous full backup before incremental backup", JET_err.MissingFullBackup)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingFullBackupException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingFullBackupException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadBackupDatabaseSize exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadBackupDatabaseSizeException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadBackupDatabaseSizeException class.
        /// </summary>
        public EsentBadBackupDatabaseSizeException() :
            base("The backup database size is not in 4k", JET_err.BadBackupDatabaseSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadBackupDatabaseSizeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadBackupDatabaseSizeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseAlreadyUpgraded exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseAlreadyUpgradedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseAlreadyUpgradedException class.
        /// </summary>
        public EsentDatabaseAlreadyUpgradedException() :
            base("Attempted to upgrade a database that is already current", JET_err.DatabaseAlreadyUpgraded)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseAlreadyUpgradedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseAlreadyUpgradedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseIncompleteUpgrade exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseIncompleteUpgradeException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseIncompleteUpgradeException class.
        /// </summary>
        public EsentDatabaseIncompleteUpgradeException() :
            base("Attempted to use a database which was only partially converted to the current format -- must restore from backup", JET_err.DatabaseIncompleteUpgrade)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseIncompleteUpgradeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseIncompleteUpgradeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingCurrentLogFiles exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingCurrentLogFilesException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingCurrentLogFilesException class.
        /// </summary>
        public EsentMissingCurrentLogFilesException() :
            base("Some current log files are missing for continuous restore", JET_err.MissingCurrentLogFiles)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingCurrentLogFilesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingCurrentLogFilesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DbTimeTooOld exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDbTimeTooOldException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDbTimeTooOldException class.
        /// </summary>
        public EsentDbTimeTooOldException() :
            base("dbtime on page smaller than dbtimeBefore in record", JET_err.DbTimeTooOld)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDbTimeTooOldException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDbTimeTooOldException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DbTimeTooNew exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDbTimeTooNewException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDbTimeTooNewException class.
        /// </summary>
        public EsentDbTimeTooNewException() :
            base("dbtime on page in advance of the dbtimeBefore in record", JET_err.DbTimeTooNew)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDbTimeTooNewException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDbTimeTooNewException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MissingFileToBackup exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMissingFileToBackupException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMissingFileToBackupException class.
        /// </summary>
        public EsentMissingFileToBackupException() :
            base("Some log or patch files are missing during backup", JET_err.MissingFileToBackup)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMissingFileToBackupException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMissingFileToBackupException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogTornWriteDuringHardRestore exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogTornWriteDuringHardRestoreException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogTornWriteDuringHardRestoreException class.
        /// </summary>
        public EsentLogTornWriteDuringHardRestoreException() :
            base("torn-write was detected in a backup set during hard restore", JET_err.LogTornWriteDuringHardRestore)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogTornWriteDuringHardRestoreException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogTornWriteDuringHardRestoreException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogTornWriteDuringHardRecovery exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogTornWriteDuringHardRecoveryException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogTornWriteDuringHardRecoveryException class.
        /// </summary>
        public EsentLogTornWriteDuringHardRecoveryException() :
            base("torn-write was detected during hard recovery (log was not part of a backup set)", JET_err.LogTornWriteDuringHardRecovery)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogTornWriteDuringHardRecoveryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogTornWriteDuringHardRecoveryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogCorruptDuringHardRestore exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogCorruptDuringHardRestoreException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogCorruptDuringHardRestoreException class.
        /// </summary>
        public EsentLogCorruptDuringHardRestoreException() :
            base("corruption was detected in a backup set during hard restore", JET_err.LogCorruptDuringHardRestore)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogCorruptDuringHardRestoreException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogCorruptDuringHardRestoreException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogCorruptDuringHardRecovery exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogCorruptDuringHardRecoveryException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogCorruptDuringHardRecoveryException class.
        /// </summary>
        public EsentLogCorruptDuringHardRecoveryException() :
            base("corruption was detected during hard recovery (log was not part of a backup set)", JET_err.LogCorruptDuringHardRecovery)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogCorruptDuringHardRecoveryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogCorruptDuringHardRecoveryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MustDisableLoggingForDbUpgrade exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMustDisableLoggingForDbUpgradeException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMustDisableLoggingForDbUpgradeException class.
        /// </summary>
        public EsentMustDisableLoggingForDbUpgradeException() :
            base("Cannot have logging enabled while attempting to upgrade db", JET_err.MustDisableLoggingForDbUpgrade)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMustDisableLoggingForDbUpgradeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMustDisableLoggingForDbUpgradeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadRestoreTargetInstance exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadRestoreTargetInstanceException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadRestoreTargetInstanceException class.
        /// </summary>
        public EsentBadRestoreTargetInstanceException() :
            base("TargetInstance specified for restore is not found or log files don't match", JET_err.BadRestoreTargetInstance)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadRestoreTargetInstanceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadRestoreTargetInstanceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecoveredWithoutUndo exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecoveredWithoutUndoException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecoveredWithoutUndoException class.
        /// </summary>
        public EsentRecoveredWithoutUndoException() :
            base("Soft recovery successfully replayed all operations, but the Undo phase of recovery was skipped", JET_err.RecoveredWithoutUndo)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecoveredWithoutUndoException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecoveredWithoutUndoException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabasesNotFromSameSnapshot exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabasesNotFromSameSnapshotException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabasesNotFromSameSnapshotException class.
        /// </summary>
        public EsentDatabasesNotFromSameSnapshotException() :
            base("Databases to be restored are not from the same shadow copy backup", JET_err.DatabasesNotFromSameSnapshot)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabasesNotFromSameSnapshotException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabasesNotFromSameSnapshotException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SoftRecoveryOnSnapshot exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSoftRecoveryOnSnapshotException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSoftRecoveryOnSnapshotException class.
        /// </summary>
        public EsentSoftRecoveryOnSnapshotException() :
            base("Soft recovery on a database from a shadow copy backup set", JET_err.SoftRecoveryOnSnapshot)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSoftRecoveryOnSnapshotException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSoftRecoveryOnSnapshotException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CommittedLogFilesMissing exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCommittedLogFilesMissingException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCommittedLogFilesMissingException class.
        /// </summary>
        public EsentCommittedLogFilesMissingException() :
            base("One or more logs that were committed to this database, are missing.  These log files are required to maintain durable ACID semantics, but not required to maintain consistency if the JET_bitReplayIgnoreLostLogs bit is specified during recovery.", JET_err.CommittedLogFilesMissing)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCommittedLogFilesMissingException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCommittedLogFilesMissingException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SectorSizeNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSectorSizeNotSupportedException : EsentFatalException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSectorSizeNotSupportedException class.
        /// </summary>
        public EsentSectorSizeNotSupportedException() :
            base("The physical sector size reported by the disk subsystem, is unsupported by ESE for a specific file type.", JET_err.SectorSizeNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSectorSizeNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSectorSizeNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecoveredWithoutUndoDatabasesConsistent exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecoveredWithoutUndoDatabasesConsistentException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecoveredWithoutUndoDatabasesConsistentException class.
        /// </summary>
        public EsentRecoveredWithoutUndoDatabasesConsistentException() :
            base("Soft recovery successfully replayed all operations and intended to skip the Undo phase of recovery, but the Undo phase was not required", JET_err.RecoveredWithoutUndoDatabasesConsistent)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecoveredWithoutUndoDatabasesConsistentException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecoveredWithoutUndoDatabasesConsistentException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CommittedLogFileCorrupt exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCommittedLogFileCorruptException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCommittedLogFileCorruptException class.
        /// </summary>
        public EsentCommittedLogFileCorruptException() :
            base("One or more logs were found to be corrupt during recovery.  These log files are required to maintain durable ACID semantics, but not required to maintain consistency if the JET_bitIgnoreLostLogs bit and JET_paramDeleteOutOfRangeLogs is specified during recovery.", JET_err.CommittedLogFileCorrupt)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCommittedLogFileCorruptException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCommittedLogFileCorruptException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.UnicodeTranslationBufferTooSmall exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentUnicodeTranslationBufferTooSmallException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUnicodeTranslationBufferTooSmallException class.
        /// </summary>
        public EsentUnicodeTranslationBufferTooSmallException() :
            base("Unicode translation buffer too small", JET_err.UnicodeTranslationBufferTooSmall)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUnicodeTranslationBufferTooSmallException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentUnicodeTranslationBufferTooSmallException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.UnicodeTranslationFail exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentUnicodeTranslationFailException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUnicodeTranslationFailException class.
        /// </summary>
        public EsentUnicodeTranslationFailException() :
            base("Unicode normalization failed", JET_err.UnicodeTranslationFail)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUnicodeTranslationFailException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentUnicodeTranslationFailException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.UnicodeNormalizationNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentUnicodeNormalizationNotSupportedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUnicodeNormalizationNotSupportedException class.
        /// </summary>
        public EsentUnicodeNormalizationNotSupportedException() :
            base("OS does not provide support for Unicode normalisation (and no normalisation callback was specified)", JET_err.UnicodeNormalizationNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUnicodeNormalizationNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentUnicodeNormalizationNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.UnicodeLanguageValidationFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentUnicodeLanguageValidationFailureException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUnicodeLanguageValidationFailureException class.
        /// </summary>
        public EsentUnicodeLanguageValidationFailureException() :
            base("Can not validate the language", JET_err.UnicodeLanguageValidationFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUnicodeLanguageValidationFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentUnicodeLanguageValidationFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ExistingLogFileHasBadSignature exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentExistingLogFileHasBadSignatureException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentExistingLogFileHasBadSignatureException class.
        /// </summary>
        public EsentExistingLogFileHasBadSignatureException() :
            base("Existing log file has bad signature", JET_err.ExistingLogFileHasBadSignature)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentExistingLogFileHasBadSignatureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentExistingLogFileHasBadSignatureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ExistingLogFileIsNotContiguous exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentExistingLogFileIsNotContiguousException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentExistingLogFileIsNotContiguousException class.
        /// </summary>
        public EsentExistingLogFileIsNotContiguousException() :
            base("Existing log file is not contiguous", JET_err.ExistingLogFileIsNotContiguous)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentExistingLogFileIsNotContiguousException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentExistingLogFileIsNotContiguousException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogReadVerifyFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogReadVerifyFailureException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogReadVerifyFailureException class.
        /// </summary>
        public EsentLogReadVerifyFailureException() :
            base("Checksum error in log file during backup", JET_err.LogReadVerifyFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogReadVerifyFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogReadVerifyFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVReadVerifyFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVReadVerifyFailureException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVReadVerifyFailureException class.
        /// </summary>
        public EsentSLVReadVerifyFailureException() :
            base("Checksum error in SLV file during backup", JET_err.SLVReadVerifyFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVReadVerifyFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVReadVerifyFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CheckpointDepthTooDeep exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCheckpointDepthTooDeepException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCheckpointDepthTooDeepException class.
        /// </summary>
        public EsentCheckpointDepthTooDeepException() :
            base("too many outstanding generations between checkpoint and current generation", JET_err.CheckpointDepthTooDeep)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCheckpointDepthTooDeepException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCheckpointDepthTooDeepException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RestoreOfNonBackupDatabase exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRestoreOfNonBackupDatabaseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRestoreOfNonBackupDatabaseException class.
        /// </summary>
        public EsentRestoreOfNonBackupDatabaseException() :
            base("hard recovery attempted on a database that wasn't a backup database", JET_err.RestoreOfNonBackupDatabase)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRestoreOfNonBackupDatabaseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRestoreOfNonBackupDatabaseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogFileNotCopied exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogFileNotCopiedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogFileNotCopiedException class.
        /// </summary>
        public EsentLogFileNotCopiedException() :
            base("log truncation attempted but not all required logs were copied", JET_err.LogFileNotCopied)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogFileNotCopiedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogFileNotCopiedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SurrogateBackupInProgress exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSurrogateBackupInProgressException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSurrogateBackupInProgressException class.
        /// </summary>
        public EsentSurrogateBackupInProgressException() :
            base("A surrogate backup is in progress.", JET_err.SurrogateBackupInProgress)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSurrogateBackupInProgressException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSurrogateBackupInProgressException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BackupAbortByServer exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBackupAbortByServerException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBackupAbortByServerException class.
        /// </summary>
        public EsentBackupAbortByServerException() :
            base("Backup was aborted by server by calling JetTerm with JET_bitTermStopBackup or by calling JetStopBackup", JET_err.BackupAbortByServer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBackupAbortByServerException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBackupAbortByServerException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidGrbit exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidGrbitException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidGrbitException class.
        /// </summary>
        public EsentInvalidGrbitException() :
            base("Invalid flags parameter", JET_err.InvalidGrbit)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidGrbitException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidGrbitException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TermInProgress exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTermInProgressException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTermInProgressException class.
        /// </summary>
        public EsentTermInProgressException() :
            base("Termination in progress", JET_err.TermInProgress)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTermInProgressException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTermInProgressException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FeatureNotAvailable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFeatureNotAvailableException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFeatureNotAvailableException class.
        /// </summary>
        public EsentFeatureNotAvailableException() :
            base("API not supported", JET_err.FeatureNotAvailable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFeatureNotAvailableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFeatureNotAvailableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidName exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidNameException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidNameException class.
        /// </summary>
        public EsentInvalidNameException() :
            base("Invalid name", JET_err.InvalidName)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidNameException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidNameException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidParameter exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidParameterException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidParameterException class.
        /// </summary>
        public EsentInvalidParameterException() :
            base("Invalid API parameter", JET_err.InvalidParameter)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidParameterException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidParameterException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseFileReadOnly exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseFileReadOnlyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseFileReadOnlyException class.
        /// </summary>
        public EsentDatabaseFileReadOnlyException() :
            base("Tried to attach a read-only database file for read/write operations", JET_err.DatabaseFileReadOnly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseFileReadOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseFileReadOnlyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidDatabaseId exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidDatabaseIdException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidDatabaseIdException class.
        /// </summary>
        public EsentInvalidDatabaseIdException() :
            base("Invalid database id", JET_err.InvalidDatabaseId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidDatabaseIdException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidDatabaseIdException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfMemory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfMemoryException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfMemoryException class.
        /// </summary>
        public EsentOutOfMemoryException() :
            base("Out of Memory", JET_err.OutOfMemory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfMemoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfMemoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfDatabaseSpace exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfDatabaseSpaceException : EsentQuotaException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfDatabaseSpaceException class.
        /// </summary>
        public EsentOutOfDatabaseSpaceException() :
            base("Maximum database size reached", JET_err.OutOfDatabaseSpace)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfDatabaseSpaceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfDatabaseSpaceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfCursors exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfCursorsException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfCursorsException class.
        /// </summary>
        public EsentOutOfCursorsException() :
            base("Out of table cursors", JET_err.OutOfCursors)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfCursorsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfCursorsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfBuffers exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfBuffersException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfBuffersException class.
        /// </summary>
        public EsentOutOfBuffersException() :
            base("Out of database page buffers", JET_err.OutOfBuffers)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfBuffersException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfBuffersException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyIndexes exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyIndexesException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyIndexesException class.
        /// </summary>
        public EsentTooManyIndexesException() :
            base("Too many indexes", JET_err.TooManyIndexes)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyIndexesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyIndexesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyKeys exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyKeysException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyKeysException class.
        /// </summary>
        public EsentTooManyKeysException() :
            base("Too many columns in an index", JET_err.TooManyKeys)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyKeysException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyKeysException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordDeleted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordDeletedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordDeletedException class.
        /// </summary>
        public EsentRecordDeletedException() :
            base("Record has been deleted", JET_err.RecordDeleted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordDeletedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordDeletedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ReadVerifyFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentReadVerifyFailureException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentReadVerifyFailureException class.
        /// </summary>
        public EsentReadVerifyFailureException() :
            base("Checksum error on a database page", JET_err.ReadVerifyFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentReadVerifyFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentReadVerifyFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PageNotInitialized exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPageNotInitializedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPageNotInitializedException class.
        /// </summary>
        public EsentPageNotInitializedException() :
            base("Blank database page", JET_err.PageNotInitialized)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPageNotInitializedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPageNotInitializedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfFileHandles exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfFileHandlesException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfFileHandlesException class.
        /// </summary>
        public EsentOutOfFileHandlesException() :
            base("Out of file handles", JET_err.OutOfFileHandles)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfFileHandlesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfFileHandlesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DiskReadVerificationFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDiskReadVerificationFailureException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDiskReadVerificationFailureException class.
        /// </summary>
        public EsentDiskReadVerificationFailureException() :
            base("The OS returned ERROR_CRC from file IO", JET_err.DiskReadVerificationFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDiskReadVerificationFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDiskReadVerificationFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DiskIO exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDiskIOException : EsentIOException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDiskIOException class.
        /// </summary>
        public EsentDiskIOException() :
            base("Disk IO error", JET_err.DiskIO)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDiskIOException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDiskIOException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidPath exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidPathException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidPathException class.
        /// </summary>
        public EsentInvalidPathException() :
            base("Invalid file path", JET_err.InvalidPath)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidPathException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidPathException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidSystemPath exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidSystemPathException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidSystemPathException class.
        /// </summary>
        public EsentInvalidSystemPathException() :
            base("Invalid system path", JET_err.InvalidSystemPath)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidSystemPathException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidSystemPathException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidLogDirectory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidLogDirectoryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidLogDirectoryException class.
        /// </summary>
        public EsentInvalidLogDirectoryException() :
            base("Invalid log directory", JET_err.InvalidLogDirectory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidLogDirectoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidLogDirectoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordTooBig exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordTooBigException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordTooBigException class.
        /// </summary>
        public EsentRecordTooBigException() :
            base("Record larger than maximum size", JET_err.RecordTooBig)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordTooBigException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordTooBigException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyOpenDatabases exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyOpenDatabasesException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenDatabasesException class.
        /// </summary>
        public EsentTooManyOpenDatabasesException() :
            base("Too many open databases", JET_err.TooManyOpenDatabases)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenDatabasesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyOpenDatabasesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidDatabase exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidDatabaseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidDatabaseException class.
        /// </summary>
        public EsentInvalidDatabaseException() :
            base("Not a database file", JET_err.InvalidDatabase)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidDatabaseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidDatabaseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NotInitialized exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNotInitializedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNotInitializedException class.
        /// </summary>
        public EsentNotInitializedException() :
            base("Database engine not initialized", JET_err.NotInitialized)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNotInitializedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNotInitializedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.AlreadyInitialized exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentAlreadyInitializedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentAlreadyInitializedException class.
        /// </summary>
        public EsentAlreadyInitializedException() :
            base("Database engine already initialized", JET_err.AlreadyInitialized)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentAlreadyInitializedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentAlreadyInitializedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InitInProgress exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInitInProgressException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInitInProgressException class.
        /// </summary>
        public EsentInitInProgressException() :
            base("Database engine is being initialized", JET_err.InitInProgress)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInitInProgressException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInitInProgressException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileAccessDenied exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileAccessDeniedException : EsentIOException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileAccessDeniedException class.
        /// </summary>
        public EsentFileAccessDeniedException() :
            base("Cannot access file, the file is locked or in use", JET_err.FileAccessDenied)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileAccessDeniedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileAccessDeniedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.QueryNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentQueryNotSupportedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentQueryNotSupportedException class.
        /// </summary>
        public EsentQueryNotSupportedException() :
            base("Query support unavailable", JET_err.QueryNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentQueryNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentQueryNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SQLLinkNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSQLLinkNotSupportedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSQLLinkNotSupportedException class.
        /// </summary>
        public EsentSQLLinkNotSupportedException() :
            base("SQL Link support unavailable", JET_err.SQLLinkNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSQLLinkNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSQLLinkNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BufferTooSmall exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBufferTooSmallException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBufferTooSmallException class.
        /// </summary>
        public EsentBufferTooSmallException() :
            base("Buffer is too small", JET_err.BufferTooSmall)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBufferTooSmallException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBufferTooSmallException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyColumns exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyColumnsException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyColumnsException class.
        /// </summary>
        public EsentTooManyColumnsException() :
            base("Too many columns defined", JET_err.TooManyColumns)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyColumnsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyColumnsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ContainerNotEmpty exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentContainerNotEmptyException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentContainerNotEmptyException class.
        /// </summary>
        public EsentContainerNotEmptyException() :
            base("Container is not empty", JET_err.ContainerNotEmpty)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentContainerNotEmptyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentContainerNotEmptyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidFilename exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidFilenameException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidFilenameException class.
        /// </summary>
        public EsentInvalidFilenameException() :
            base("Filename is invalid", JET_err.InvalidFilename)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidFilenameException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidFilenameException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidBookmark exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidBookmarkException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidBookmarkException class.
        /// </summary>
        public EsentInvalidBookmarkException() :
            base("Invalid bookmark", JET_err.InvalidBookmark)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidBookmarkException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidBookmarkException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnInUseException class.
        /// </summary>
        public EsentColumnInUseException() :
            base("Column used in an index", JET_err.ColumnInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidBufferSize exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidBufferSizeException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidBufferSizeException class.
        /// </summary>
        public EsentInvalidBufferSizeException() :
            base("Data buffer doesn't match column size", JET_err.InvalidBufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidBufferSizeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidBufferSizeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnNotUpdatable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnNotUpdatableException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnNotUpdatableException class.
        /// </summary>
        public EsentColumnNotUpdatableException() :
            base("Cannot set column value", JET_err.ColumnNotUpdatable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnNotUpdatableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnNotUpdatableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexInUseException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexInUseException class.
        /// </summary>
        public EsentIndexInUseException() :
            base("Index is in use", JET_err.IndexInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LinkNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLinkNotSupportedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLinkNotSupportedException class.
        /// </summary>
        public EsentLinkNotSupportedException() :
            base("Link support unavailable", JET_err.LinkNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLinkNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLinkNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NullKeyDisallowed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNullKeyDisallowedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNullKeyDisallowedException class.
        /// </summary>
        public EsentNullKeyDisallowedException() :
            base("Null keys are disallowed on index", JET_err.NullKeyDisallowed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNullKeyDisallowedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNullKeyDisallowedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NotInTransaction exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNotInTransactionException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNotInTransactionException class.
        /// </summary>
        public EsentNotInTransactionException() :
            base("Operation must be within a transaction", JET_err.NotInTransaction)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNotInTransactionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNotInTransactionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MustRollback exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMustRollbackException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMustRollbackException class.
        /// </summary>
        public EsentMustRollbackException() :
            base("Transaction must rollback because failure of unversioned update", JET_err.MustRollback)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMustRollbackException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMustRollbackException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyActiveUsers exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyActiveUsersException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyActiveUsersException class.
        /// </summary>
        public EsentTooManyActiveUsersException() :
            base("Too many active database users", JET_err.TooManyActiveUsers)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyActiveUsersException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyActiveUsersException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidCountry exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidCountryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidCountryException class.
        /// </summary>
        public EsentInvalidCountryException() :
            base("Invalid or unknown country/region code", JET_err.InvalidCountry)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidCountryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidCountryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidLanguageId exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidLanguageIdException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidLanguageIdException class.
        /// </summary>
        public EsentInvalidLanguageIdException() :
            base("Invalid or unknown language id", JET_err.InvalidLanguageId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidLanguageIdException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidLanguageIdException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidCodePage exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidCodePageException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidCodePageException class.
        /// </summary>
        public EsentInvalidCodePageException() :
            base("Invalid or unknown code page", JET_err.InvalidCodePage)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidCodePageException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidCodePageException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidLCMapStringFlags exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidLCMapStringFlagsException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidLCMapStringFlagsException class.
        /// </summary>
        public EsentInvalidLCMapStringFlagsException() :
            base("Invalid flags for LCMapString()", JET_err.InvalidLCMapStringFlags)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidLCMapStringFlagsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidLCMapStringFlagsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.VersionStoreEntryTooBig exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentVersionStoreEntryTooBigException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentVersionStoreEntryTooBigException class.
        /// </summary>
        public EsentVersionStoreEntryTooBigException() :
            base("Attempted to create a version store entry (RCE) larger than a version bucket", JET_err.VersionStoreEntryTooBig)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentVersionStoreEntryTooBigException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentVersionStoreEntryTooBigException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.VersionStoreOutOfMemoryAndCleanupTimedOut exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentVersionStoreOutOfMemoryAndCleanupTimedOutException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentVersionStoreOutOfMemoryAndCleanupTimedOutException class.
        /// </summary>
        public EsentVersionStoreOutOfMemoryAndCleanupTimedOutException() :
            base("Version store out of memory (and cleanup attempt failed to complete)", JET_err.VersionStoreOutOfMemoryAndCleanupTimedOut)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentVersionStoreOutOfMemoryAndCleanupTimedOutException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentVersionStoreOutOfMemoryAndCleanupTimedOutException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.VersionStoreOutOfMemory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentVersionStoreOutOfMemoryException : EsentQuotaException
    {
        /// <summary>
        /// Initializes a new instance of the EsentVersionStoreOutOfMemoryException class.
        /// </summary>
        public EsentVersionStoreOutOfMemoryException() :
            base("Version store out of memory (cleanup already attempted)", JET_err.VersionStoreOutOfMemory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentVersionStoreOutOfMemoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentVersionStoreOutOfMemoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CurrencyStackOutOfMemory exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCurrencyStackOutOfMemoryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCurrencyStackOutOfMemoryException class.
        /// </summary>
        public EsentCurrencyStackOutOfMemoryException() :
            base("UNUSED: lCSRPerfFUCB * g_lCursorsMax exceeded (XJET only)", JET_err.CurrencyStackOutOfMemory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCurrencyStackOutOfMemoryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCurrencyStackOutOfMemoryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotIndex exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotIndexException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotIndexException class.
        /// </summary>
        public EsentCannotIndexException() :
            base("Cannot index escrow column or SLV column", JET_err.CannotIndex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotIndexException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotIndexException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordNotDeleted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordNotDeletedException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordNotDeletedException class.
        /// </summary>
        public EsentRecordNotDeletedException() :
            base("Record has not been deleted", JET_err.RecordNotDeleted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordNotDeletedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordNotDeletedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyMempoolEntries exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyMempoolEntriesException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyMempoolEntriesException class.
        /// </summary>
        public EsentTooManyMempoolEntriesException() :
            base("Too many mempool entries requested", JET_err.TooManyMempoolEntries)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyMempoolEntriesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyMempoolEntriesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfObjectIDs exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfObjectIDsException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfObjectIDsException class.
        /// </summary>
        public EsentOutOfObjectIDsException() :
            base("Out of btree ObjectIDs (perform offline defrag to reclaim freed/unused ObjectIds)", JET_err.OutOfObjectIDs)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfObjectIDsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfObjectIDsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfLongValueIDs exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfLongValueIDsException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfLongValueIDsException class.
        /// </summary>
        public EsentOutOfLongValueIDsException() :
            base("Long-value ID counter has reached maximum value. (perform offline defrag to reclaim free/unused LongValueIDs)", JET_err.OutOfLongValueIDs)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfLongValueIDsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfLongValueIDsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfAutoincrementValues exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfAutoincrementValuesException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfAutoincrementValuesException class.
        /// </summary>
        public EsentOutOfAutoincrementValuesException() :
            base("Auto-increment counter has reached maximum value (offline defrag WILL NOT be able to reclaim free/unused Auto-increment values).", JET_err.OutOfAutoincrementValues)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfAutoincrementValuesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfAutoincrementValuesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfDbtimeValues exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfDbtimeValuesException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfDbtimeValuesException class.
        /// </summary>
        public EsentOutOfDbtimeValuesException() :
            base("Dbtime counter has reached maximum value (perform offline defrag to reclaim free/unused Dbtime values)", JET_err.OutOfDbtimeValues)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfDbtimeValuesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfDbtimeValuesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfSequentialIndexValues exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfSequentialIndexValuesException : EsentFragmentationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfSequentialIndexValuesException class.
        /// </summary>
        public EsentOutOfSequentialIndexValuesException() :
            base("Sequential index counter has reached maximum value (perform offline defrag to reclaim free/unused SequentialIndex values)", JET_err.OutOfSequentialIndexValues)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfSequentialIndexValuesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfSequentialIndexValuesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RunningInOneInstanceMode exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRunningInOneInstanceModeException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRunningInOneInstanceModeException class.
        /// </summary>
        public EsentRunningInOneInstanceModeException() :
            base("Multi-instance call with single-instance mode enabled", JET_err.RunningInOneInstanceMode)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRunningInOneInstanceModeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRunningInOneInstanceModeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RunningInMultiInstanceMode exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRunningInMultiInstanceModeException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRunningInMultiInstanceModeException class.
        /// </summary>
        public EsentRunningInMultiInstanceModeException() :
            base("Single-instance call with multi-instance mode enabled", JET_err.RunningInMultiInstanceMode)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRunningInMultiInstanceModeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRunningInMultiInstanceModeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SystemParamsAlreadySet exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSystemParamsAlreadySetException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSystemParamsAlreadySetException class.
        /// </summary>
        public EsentSystemParamsAlreadySetException() :
            base("Global system parameters have already been set", JET_err.SystemParamsAlreadySet)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSystemParamsAlreadySetException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSystemParamsAlreadySetException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SystemPathInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSystemPathInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSystemPathInUseException class.
        /// </summary>
        public EsentSystemPathInUseException() :
            base("System path already used by another database instance", JET_err.SystemPathInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSystemPathInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSystemPathInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogFilePathInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogFilePathInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogFilePathInUseException class.
        /// </summary>
        public EsentLogFilePathInUseException() :
            base("Logfile path already used by another database instance", JET_err.LogFilePathInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogFilePathInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogFilePathInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TempPathInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTempPathInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTempPathInUseException class.
        /// </summary>
        public EsentTempPathInUseException() :
            base("Temp path already used by another database instance", JET_err.TempPathInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTempPathInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTempPathInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InstanceNameInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInstanceNameInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInstanceNameInUseException class.
        /// </summary>
        public EsentInstanceNameInUseException() :
            base("Instance Name already in use", JET_err.InstanceNameInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInstanceNameInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInstanceNameInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InstanceUnavailable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInstanceUnavailableException : EsentFatalException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInstanceUnavailableException class.
        /// </summary>
        public EsentInstanceUnavailableException() :
            base("This instance cannot be used because it encountered a fatal error", JET_err.InstanceUnavailable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInstanceUnavailableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInstanceUnavailableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseUnavailable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseUnavailableException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseUnavailableException class.
        /// </summary>
        public EsentDatabaseUnavailableException() :
            base("This database cannot be used because it encountered a fatal error", JET_err.DatabaseUnavailable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseUnavailableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseUnavailableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InstanceUnavailableDueToFatalLogDiskFull exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInstanceUnavailableDueToFatalLogDiskFullException : EsentFatalException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInstanceUnavailableDueToFatalLogDiskFullException class.
        /// </summary>
        public EsentInstanceUnavailableDueToFatalLogDiskFullException() :
            base("This instance cannot be used because it encountered a log-disk-full error performing an operation (likely transaction rollback) that could not tolerate failure", JET_err.InstanceUnavailableDueToFatalLogDiskFull)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInstanceUnavailableDueToFatalLogDiskFullException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInstanceUnavailableDueToFatalLogDiskFullException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OutOfSessions exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOutOfSessionsException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOutOfSessionsException class.
        /// </summary>
        public EsentOutOfSessionsException() :
            base("Out of sessions", JET_err.OutOfSessions)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOutOfSessionsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOutOfSessionsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.WriteConflict exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentWriteConflictException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentWriteConflictException class.
        /// </summary>
        public EsentWriteConflictException() :
            base("Write lock failed due to outstanding write lock", JET_err.WriteConflict)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentWriteConflictException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentWriteConflictException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TransTooDeep exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTransTooDeepException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTransTooDeepException class.
        /// </summary>
        public EsentTransTooDeepException() :
            base("Transactions nested too deeply", JET_err.TransTooDeep)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTransTooDeepException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTransTooDeepException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidSesid exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidSesidException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidSesidException class.
        /// </summary>
        public EsentInvalidSesidException() :
            base("Invalid session handle", JET_err.InvalidSesid)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidSesidException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidSesidException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.WriteConflictPrimaryIndex exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentWriteConflictPrimaryIndexException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentWriteConflictPrimaryIndexException class.
        /// </summary>
        public EsentWriteConflictPrimaryIndexException() :
            base("Update attempted on uncommitted primary index", JET_err.WriteConflictPrimaryIndex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentWriteConflictPrimaryIndexException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentWriteConflictPrimaryIndexException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InTransaction exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInTransactionException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInTransactionException class.
        /// </summary>
        public EsentInTransactionException() :
            base("Operation not allowed within a transaction", JET_err.InTransaction)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInTransactionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInTransactionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RollbackRequired exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRollbackRequiredException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRollbackRequiredException class.
        /// </summary>
        public EsentRollbackRequiredException() :
            base("Must rollback current transaction -- cannot commit or begin a new one", JET_err.RollbackRequired)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRollbackRequiredException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRollbackRequiredException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TransReadOnly exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTransReadOnlyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTransReadOnlyException class.
        /// </summary>
        public EsentTransReadOnlyException() :
            base("Read-only transaction tried to modify the database", JET_err.TransReadOnly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTransReadOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTransReadOnlyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SessionWriteConflict exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSessionWriteConflictException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSessionWriteConflictException class.
        /// </summary>
        public EsentSessionWriteConflictException() :
            base("Attempt to replace the same record by two diffrerent cursors in the same session", JET_err.SessionWriteConflict)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSessionWriteConflictException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSessionWriteConflictException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordTooBigForBackwardCompatibility exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordTooBigForBackwardCompatibilityException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordTooBigForBackwardCompatibilityException class.
        /// </summary>
        public EsentRecordTooBigForBackwardCompatibilityException() :
            base("record would be too big if represented in a database format from a previous version of Jet", JET_err.RecordTooBigForBackwardCompatibility)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordTooBigForBackwardCompatibilityException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordTooBigForBackwardCompatibilityException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotMaterializeForwardOnlySort exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotMaterializeForwardOnlySortException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotMaterializeForwardOnlySortException class.
        /// </summary>
        public EsentCannotMaterializeForwardOnlySortException() :
            base("The temp table could not be created due to parameters that conflict with JET_bitTTForwardOnly", JET_err.CannotMaterializeForwardOnlySort)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotMaterializeForwardOnlySortException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotMaterializeForwardOnlySortException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SesidTableIdMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSesidTableIdMismatchException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSesidTableIdMismatchException class.
        /// </summary>
        public EsentSesidTableIdMismatchException() :
            base("This session handle can't be used with this table id", JET_err.SesidTableIdMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSesidTableIdMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSesidTableIdMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidInstance exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidInstanceException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidInstanceException class.
        /// </summary>
        public EsentInvalidInstanceException() :
            base("Invalid instance handle", JET_err.InvalidInstance)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidInstanceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidInstanceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DirtyShutdown exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDirtyShutdownException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDirtyShutdownException class.
        /// </summary>
        public EsentDirtyShutdownException() :
            base("The instance was shutdown successfully but all the attached databases were left in a dirty state by request via JET_bitTermDirty", JET_err.DirtyShutdown)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDirtyShutdownException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDirtyShutdownException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ReadPgnoVerifyFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentReadPgnoVerifyFailureException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentReadPgnoVerifyFailureException class.
        /// </summary>
        public EsentReadPgnoVerifyFailureException() :
            base("The database page read from disk had the wrong page number.", JET_err.ReadPgnoVerifyFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentReadPgnoVerifyFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentReadPgnoVerifyFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ReadLostFlushVerifyFailure exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentReadLostFlushVerifyFailureException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentReadLostFlushVerifyFailureException class.
        /// </summary>
        public EsentReadLostFlushVerifyFailureException() :
            base("The database page read from disk had a previous write not represented on the page.", JET_err.ReadLostFlushVerifyFailure)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentReadLostFlushVerifyFailureException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentReadLostFlushVerifyFailureException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MustCommitDistributedTransactionToLevel0 exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMustCommitDistributedTransactionToLevel0Exception : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMustCommitDistributedTransactionToLevel0Exception class.
        /// </summary>
        public EsentMustCommitDistributedTransactionToLevel0Exception() :
            base("Attempted to PrepareToCommit a distributed transaction to non-zero level", JET_err.MustCommitDistributedTransactionToLevel0)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMustCommitDistributedTransactionToLevel0Exception class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMustCommitDistributedTransactionToLevel0Exception(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DistributedTransactionAlreadyPreparedToCommit exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDistributedTransactionAlreadyPreparedToCommitException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDistributedTransactionAlreadyPreparedToCommitException class.
        /// </summary>
        public EsentDistributedTransactionAlreadyPreparedToCommitException() :
            base("Attempted a write-operation after a distributed transaction has called PrepareToCommit", JET_err.DistributedTransactionAlreadyPreparedToCommit)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDistributedTransactionAlreadyPreparedToCommitException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDistributedTransactionAlreadyPreparedToCommitException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NotInDistributedTransaction exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNotInDistributedTransactionException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNotInDistributedTransactionException class.
        /// </summary>
        public EsentNotInDistributedTransactionException() :
            base("Attempted to PrepareToCommit a non-distributed transaction", JET_err.NotInDistributedTransaction)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNotInDistributedTransactionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNotInDistributedTransactionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DistributedTransactionNotYetPreparedToCommit exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDistributedTransactionNotYetPreparedToCommitException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDistributedTransactionNotYetPreparedToCommitException class.
        /// </summary>
        public EsentDistributedTransactionNotYetPreparedToCommitException() :
            base("Attempted to commit a distributed transaction, but PrepareToCommit has not yet been called", JET_err.DistributedTransactionNotYetPreparedToCommit)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDistributedTransactionNotYetPreparedToCommitException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDistributedTransactionNotYetPreparedToCommitException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotNestDistributedTransactions exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotNestDistributedTransactionsException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotNestDistributedTransactionsException class.
        /// </summary>
        public EsentCannotNestDistributedTransactionsException() :
            base("Attempted to begin a distributed transaction when not at level 0", JET_err.CannotNestDistributedTransactions)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotNestDistributedTransactionsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotNestDistributedTransactionsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DTCMissingCallback exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDTCMissingCallbackException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDTCMissingCallbackException class.
        /// </summary>
        public EsentDTCMissingCallbackException() :
            base("Attempted to begin a distributed transaction but no callback for DTC coordination was specified on initialisation", JET_err.DTCMissingCallback)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDTCMissingCallbackException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDTCMissingCallbackException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DTCMissingCallbackOnRecovery exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDTCMissingCallbackOnRecoveryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDTCMissingCallbackOnRecoveryException class.
        /// </summary>
        public EsentDTCMissingCallbackOnRecoveryException() :
            base("Attempted to recover a distributed transaction but no callback for DTC coordination was specified on initialisation", JET_err.DTCMissingCallbackOnRecovery)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDTCMissingCallbackOnRecoveryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDTCMissingCallbackOnRecoveryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DTCCallbackUnexpectedError exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDTCCallbackUnexpectedErrorException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDTCCallbackUnexpectedErrorException class.
        /// </summary>
        public EsentDTCCallbackUnexpectedErrorException() :
            base("Unexpected error code returned from DTC callback", JET_err.DTCCallbackUnexpectedError)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDTCCallbackUnexpectedErrorException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDTCCallbackUnexpectedErrorException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseDuplicateException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseDuplicateException class.
        /// </summary>
        public EsentDatabaseDuplicateException() :
            base("Database already exists", JET_err.DatabaseDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInUseException class.
        /// </summary>
        public EsentDatabaseInUseException() :
            base("Database in use", JET_err.DatabaseInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseNotFoundException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseNotFoundException class.
        /// </summary>
        public EsentDatabaseNotFoundException() :
            base("No such database", JET_err.DatabaseNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseInvalidName exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseInvalidNameException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidNameException class.
        /// </summary>
        public EsentDatabaseInvalidNameException() :
            base("Invalid database name", JET_err.DatabaseInvalidName)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidNameException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseInvalidNameException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseInvalidPages exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseInvalidPagesException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidPagesException class.
        /// </summary>
        public EsentDatabaseInvalidPagesException() :
            base("Invalid number of pages", JET_err.DatabaseInvalidPages)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidPagesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseInvalidPagesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseCorruptedException class.
        /// </summary>
        public EsentDatabaseCorruptedException() :
            base("Non database file or corrupted db", JET_err.DatabaseCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseLocked exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseLockedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseLockedException class.
        /// </summary>
        public EsentDatabaseLockedException() :
            base("Database exclusively locked", JET_err.DatabaseLocked)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseLockedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseLockedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotDisableVersioning exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotDisableVersioningException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotDisableVersioningException class.
        /// </summary>
        public EsentCannotDisableVersioningException() :
            base("Cannot disable versioning for this database", JET_err.CannotDisableVersioning)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotDisableVersioningException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotDisableVersioningException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidDatabaseVersion exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidDatabaseVersionException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidDatabaseVersionException class.
        /// </summary>
        public EsentInvalidDatabaseVersionException() :
            base("Database engine is incompatible with database", JET_err.InvalidDatabaseVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidDatabaseVersionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidDatabaseVersionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.Database200Format exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabase200FormatException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabase200FormatException class.
        /// </summary>
        public EsentDatabase200FormatException() :
            base("The database is in an older (200) format", JET_err.Database200Format)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabase200FormatException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabase200FormatException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.Database400Format exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabase400FormatException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabase400FormatException class.
        /// </summary>
        public EsentDatabase400FormatException() :
            base("The database is in an older (400) format", JET_err.Database400Format)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabase400FormatException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabase400FormatException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.Database500Format exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabase500FormatException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabase500FormatException class.
        /// </summary>
        public EsentDatabase500FormatException() :
            base("The database is in an older (500) format", JET_err.Database500Format)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabase500FormatException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabase500FormatException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PageSizeMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPageSizeMismatchException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPageSizeMismatchException class.
        /// </summary>
        public EsentPageSizeMismatchException() :
            base("The database page size does not match the engine", JET_err.PageSizeMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPageSizeMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPageSizeMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyInstances exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyInstancesException : EsentQuotaException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyInstancesException class.
        /// </summary>
        public EsentTooManyInstancesException() :
            base("Cannot start any more database instances", JET_err.TooManyInstances)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyInstancesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyInstancesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseSharingViolation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseSharingViolationException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseSharingViolationException class.
        /// </summary>
        public EsentDatabaseSharingViolationException() :
            base("A different database instance is using this database", JET_err.DatabaseSharingViolation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseSharingViolationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseSharingViolationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.AttachedDatabaseMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentAttachedDatabaseMismatchException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentAttachedDatabaseMismatchException class.
        /// </summary>
        public EsentAttachedDatabaseMismatchException() :
            base("An outstanding database attachment has been detected at the start or end of recovery, but database is missing or does not match attachment info", JET_err.AttachedDatabaseMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentAttachedDatabaseMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentAttachedDatabaseMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseInvalidPath exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseInvalidPathException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidPathException class.
        /// </summary>
        public EsentDatabaseInvalidPathException() :
            base("Specified path to database file is illegal", JET_err.DatabaseInvalidPath)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidPathException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseInvalidPathException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseIdInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseIdInUseException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseIdInUseException class.
        /// </summary>
        public EsentDatabaseIdInUseException() :
            base("A database is being assigned an id already in use", JET_err.DatabaseIdInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseIdInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseIdInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ForceDetachNotAllowed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentForceDetachNotAllowedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentForceDetachNotAllowedException class.
        /// </summary>
        public EsentForceDetachNotAllowedException() :
            base("Force Detach allowed only after normal detach errored out", JET_err.ForceDetachNotAllowed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentForceDetachNotAllowedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentForceDetachNotAllowedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CatalogCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCatalogCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCatalogCorruptedException class.
        /// </summary>
        public EsentCatalogCorruptedException() :
            base("Corruption detected in catalog", JET_err.CatalogCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCatalogCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCatalogCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PartiallyAttachedDB exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPartiallyAttachedDBException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPartiallyAttachedDBException class.
        /// </summary>
        public EsentPartiallyAttachedDBException() :
            base("Database is partially attached. Cannot complete attach operation", JET_err.PartiallyAttachedDB)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPartiallyAttachedDBException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPartiallyAttachedDBException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseSignInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseSignInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseSignInUseException class.
        /// </summary>
        public EsentDatabaseSignInUseException() :
            base("Database with same signature in use", JET_err.DatabaseSignInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseSignInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseSignInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseCorruptedNoRepair exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseCorruptedNoRepairException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseCorruptedNoRepairException class.
        /// </summary>
        public EsentDatabaseCorruptedNoRepairException() :
            base("Corrupted db but repair not allowed", JET_err.DatabaseCorruptedNoRepair)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseCorruptedNoRepairException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseCorruptedNoRepairException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidCreateDbVersion exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidCreateDbVersionException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidCreateDbVersionException class.
        /// </summary>
        public EsentInvalidCreateDbVersionException() :
            base("recovery tried to replay a database creation, but the database was originally created with an incompatible (likely older) version of the database engine", JET_err.InvalidCreateDbVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidCreateDbVersionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidCreateDbVersionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseIncompleteIncrementalReseed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseIncompleteIncrementalReseedException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseIncompleteIncrementalReseedException class.
        /// </summary>
        public EsentDatabaseIncompleteIncrementalReseedException() :
            base("The database cannot be attached because it is currently being rebuilt as part of an incremental reseed.", JET_err.DatabaseIncompleteIncrementalReseed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseIncompleteIncrementalReseedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseIncompleteIncrementalReseedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseInvalidIncrementalReseed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseInvalidIncrementalReseedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidIncrementalReseedException class.
        /// </summary>
        public EsentDatabaseInvalidIncrementalReseedException() :
            base("The database is not a valid state to perform an incremental reseed.", JET_err.DatabaseInvalidIncrementalReseed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseInvalidIncrementalReseedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseInvalidIncrementalReseedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseFailedIncrementalReseed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseFailedIncrementalReseedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseFailedIncrementalReseedException class.
        /// </summary>
        public EsentDatabaseFailedIncrementalReseedException() :
            base("The incremental reseed being performed on the specified database cannot be completed due to a fatal error.  A full reseed is required to recover this database.", JET_err.DatabaseFailedIncrementalReseed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseFailedIncrementalReseedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseFailedIncrementalReseedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NoAttachmentsFailedIncrementalReseed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNoAttachmentsFailedIncrementalReseedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNoAttachmentsFailedIncrementalReseedException class.
        /// </summary>
        public EsentNoAttachmentsFailedIncrementalReseedException() :
            base("The incremental reseed being performed on the specified database cannot be completed because the min required log contains no attachment info.  A full reseed is required to recover this database.", JET_err.NoAttachmentsFailedIncrementalReseed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNoAttachmentsFailedIncrementalReseedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNoAttachmentsFailedIncrementalReseedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TableLocked exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTableLockedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTableLockedException class.
        /// </summary>
        public EsentTableLockedException() :
            base("Table is exclusively locked", JET_err.TableLocked)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTableLockedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTableLockedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TableDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTableDuplicateException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTableDuplicateException class.
        /// </summary>
        public EsentTableDuplicateException() :
            base("Table already exists", JET_err.TableDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTableDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTableDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TableInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTableInUseException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTableInUseException class.
        /// </summary>
        public EsentTableInUseException() :
            base("Table is in use, cannot lock", JET_err.TableInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTableInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTableInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ObjectNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentObjectNotFoundException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentObjectNotFoundException class.
        /// </summary>
        public EsentObjectNotFoundException() :
            base("No such table or object", JET_err.ObjectNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentObjectNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentObjectNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DensityInvalid exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDensityInvalidException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDensityInvalidException class.
        /// </summary>
        public EsentDensityInvalidException() :
            base("Bad file/index density", JET_err.DensityInvalid)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDensityInvalidException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDensityInvalidException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TableNotEmpty exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTableNotEmptyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTableNotEmptyException class.
        /// </summary>
        public EsentTableNotEmptyException() :
            base("Table is not empty", JET_err.TableNotEmpty)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTableNotEmptyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTableNotEmptyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidTableId exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidTableIdException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidTableIdException class.
        /// </summary>
        public EsentInvalidTableIdException() :
            base("Invalid table id", JET_err.InvalidTableId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidTableIdException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidTableIdException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyOpenTables exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyOpenTablesException : EsentQuotaException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenTablesException class.
        /// </summary>
        public EsentTooManyOpenTablesException() :
            base("Cannot open any more tables (cleanup already attempted)", JET_err.TooManyOpenTables)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenTablesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyOpenTablesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IllegalOperation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIllegalOperationException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIllegalOperationException class.
        /// </summary>
        public EsentIllegalOperationException() :
            base("Oper. not supported on table", JET_err.IllegalOperation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIllegalOperationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIllegalOperationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyOpenTablesAndCleanupTimedOut exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyOpenTablesAndCleanupTimedOutException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenTablesAndCleanupTimedOutException class.
        /// </summary>
        public EsentTooManyOpenTablesAndCleanupTimedOutException() :
            base("Cannot open any more tables (cleanup attempt failed to complete)", JET_err.TooManyOpenTablesAndCleanupTimedOut)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenTablesAndCleanupTimedOutException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyOpenTablesAndCleanupTimedOutException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ObjectDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentObjectDuplicateException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentObjectDuplicateException class.
        /// </summary>
        public EsentObjectDuplicateException() :
            base("Table or object name in use", JET_err.ObjectDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentObjectDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentObjectDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidObject exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidObjectException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidObjectException class.
        /// </summary>
        public EsentInvalidObjectException() :
            base("Object is invalid for operation", JET_err.InvalidObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidObjectException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidObjectException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotDeleteTempTable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotDeleteTempTableException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotDeleteTempTableException class.
        /// </summary>
        public EsentCannotDeleteTempTableException() :
            base("Use CloseTable instead of DeleteTable to delete temp table", JET_err.CannotDeleteTempTable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotDeleteTempTableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotDeleteTempTableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotDeleteSystemTable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotDeleteSystemTableException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotDeleteSystemTableException class.
        /// </summary>
        public EsentCannotDeleteSystemTableException() :
            base("Illegal attempt to delete a system table", JET_err.CannotDeleteSystemTable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotDeleteSystemTableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotDeleteSystemTableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotDeleteTemplateTable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotDeleteTemplateTableException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotDeleteTemplateTableException class.
        /// </summary>
        public EsentCannotDeleteTemplateTableException() :
            base("Illegal attempt to delete a template table", JET_err.CannotDeleteTemplateTable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotDeleteTemplateTableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotDeleteTemplateTableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ExclusiveTableLockRequired exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentExclusiveTableLockRequiredException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentExclusiveTableLockRequiredException class.
        /// </summary>
        public EsentExclusiveTableLockRequiredException() :
            base("Must have exclusive lock on table.", JET_err.ExclusiveTableLockRequired)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentExclusiveTableLockRequiredException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentExclusiveTableLockRequiredException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FixedDDL exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFixedDDLException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFixedDDLException class.
        /// </summary>
        public EsentFixedDDLException() :
            base("DDL operations prohibited on this table", JET_err.FixedDDL)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFixedDDLException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFixedDDLException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FixedInheritedDDL exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFixedInheritedDDLException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFixedInheritedDDLException class.
        /// </summary>
        public EsentFixedInheritedDDLException() :
            base("On a derived table, DDL operations are prohibited on inherited portion of DDL", JET_err.FixedInheritedDDL)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFixedInheritedDDLException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFixedInheritedDDLException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotNestDDL exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotNestDDLException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotNestDDLException class.
        /// </summary>
        public EsentCannotNestDDLException() :
            base("Nesting of hierarchical DDL is not currently supported.", JET_err.CannotNestDDL)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotNestDDLException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotNestDDLException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DDLNotInheritable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDDLNotInheritableException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDDLNotInheritableException class.
        /// </summary>
        public EsentDDLNotInheritableException() :
            base("Tried to inherit DDL from a table not marked as a template table.", JET_err.DDLNotInheritable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDDLNotInheritableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDDLNotInheritableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidSettings exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidSettingsException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidSettingsException class.
        /// </summary>
        public EsentInvalidSettingsException() :
            base("System parameters were set improperly", JET_err.InvalidSettings)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidSettingsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidSettingsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ClientRequestToStopJetService exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentClientRequestToStopJetServiceException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentClientRequestToStopJetServiceException class.
        /// </summary>
        public EsentClientRequestToStopJetServiceException() :
            base("Client has requested stop service", JET_err.ClientRequestToStopJetService)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentClientRequestToStopJetServiceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentClientRequestToStopJetServiceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotAddFixedVarColumnToDerivedTable exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotAddFixedVarColumnToDerivedTableException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotAddFixedVarColumnToDerivedTableException class.
        /// </summary>
        public EsentCannotAddFixedVarColumnToDerivedTableException() :
            base("Template table was created with NoFixedVarColumnsInDerivedTables", JET_err.CannotAddFixedVarColumnToDerivedTable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotAddFixedVarColumnToDerivedTableException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotAddFixedVarColumnToDerivedTableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexCantBuild exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexCantBuildException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexCantBuildException class.
        /// </summary>
        public EsentIndexCantBuildException() :
            base("Index build failed", JET_err.IndexCantBuild)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexCantBuildException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexCantBuildException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexHasPrimary exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexHasPrimaryException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexHasPrimaryException class.
        /// </summary>
        public EsentIndexHasPrimaryException() :
            base("Primary index already defined", JET_err.IndexHasPrimary)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexHasPrimaryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexHasPrimaryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexDuplicateException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexDuplicateException class.
        /// </summary>
        public EsentIndexDuplicateException() :
            base("Index is already defined", JET_err.IndexDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexNotFoundException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexNotFoundException class.
        /// </summary>
        public EsentIndexNotFoundException() :
            base("No such index", JET_err.IndexNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexMustStay exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexMustStayException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexMustStayException class.
        /// </summary>
        public EsentIndexMustStayException() :
            base("Cannot delete clustered index", JET_err.IndexMustStay)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexMustStayException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexMustStayException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexInvalidDef exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexInvalidDefException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexInvalidDefException class.
        /// </summary>
        public EsentIndexInvalidDefException() :
            base("Illegal index definition", JET_err.IndexInvalidDef)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexInvalidDefException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexInvalidDefException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidCreateIndex exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidCreateIndexException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidCreateIndexException class.
        /// </summary>
        public EsentInvalidCreateIndexException() :
            base("Invalid create index description", JET_err.InvalidCreateIndex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidCreateIndexException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidCreateIndexException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyOpenIndexes exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyOpenIndexesException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenIndexesException class.
        /// </summary>
        public EsentTooManyOpenIndexesException() :
            base("Out of index description blocks", JET_err.TooManyOpenIndexes)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyOpenIndexesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyOpenIndexesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MultiValuedIndexViolation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMultiValuedIndexViolationException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedIndexViolationException class.
        /// </summary>
        public EsentMultiValuedIndexViolationException() :
            base("Non-unique inter-record index keys generated for a multivalued index", JET_err.MultiValuedIndexViolation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedIndexViolationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMultiValuedIndexViolationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexBuildCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexBuildCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexBuildCorruptedException class.
        /// </summary>
        public EsentIndexBuildCorruptedException() :
            base("Failed to build a secondary index that properly reflects primary index", JET_err.IndexBuildCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexBuildCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexBuildCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PrimaryIndexCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPrimaryIndexCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPrimaryIndexCorruptedException class.
        /// </summary>
        public EsentPrimaryIndexCorruptedException() :
            base("Primary index is corrupt. The database must be defragmented", JET_err.PrimaryIndexCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPrimaryIndexCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPrimaryIndexCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SecondaryIndexCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSecondaryIndexCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSecondaryIndexCorruptedException class.
        /// </summary>
        public EsentSecondaryIndexCorruptedException() :
            base("Secondary index is corrupt. The database must be defragmented", JET_err.SecondaryIndexCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSecondaryIndexCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSecondaryIndexCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidIndexId exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidIndexIdException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidIndexIdException class.
        /// </summary>
        public EsentInvalidIndexIdException() :
            base("Illegal index id", JET_err.InvalidIndexId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidIndexIdException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidIndexIdException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesSecondaryIndexOnly exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesSecondaryIndexOnlyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesSecondaryIndexOnlyException class.
        /// </summary>
        public EsentIndexTuplesSecondaryIndexOnlyException() :
            base("tuple index can only be on a secondary index", JET_err.IndexTuplesSecondaryIndexOnly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesSecondaryIndexOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesSecondaryIndexOnlyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesTooManyColumns exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesTooManyColumnsException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesTooManyColumnsException class.
        /// </summary>
        public EsentIndexTuplesTooManyColumnsException() :
            base("tuple index may only have eleven columns in the index", JET_err.IndexTuplesTooManyColumns)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesTooManyColumnsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesTooManyColumnsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesNonUniqueOnly exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesNonUniqueOnlyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesNonUniqueOnlyException class.
        /// </summary>
        public EsentIndexTuplesNonUniqueOnlyException() :
            base("tuple index must be a non-unique index", JET_err.IndexTuplesNonUniqueOnly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesNonUniqueOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesNonUniqueOnlyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesTextBinaryColumnsOnly exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesTextBinaryColumnsOnlyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesTextBinaryColumnsOnlyException class.
        /// </summary>
        public EsentIndexTuplesTextBinaryColumnsOnlyException() :
            base("tuple index must be on a text/binary column", JET_err.IndexTuplesTextBinaryColumnsOnly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesTextBinaryColumnsOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesTextBinaryColumnsOnlyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesVarSegMacNotAllowed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesVarSegMacNotAllowedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesVarSegMacNotAllowedException class.
        /// </summary>
        public EsentIndexTuplesVarSegMacNotAllowedException() :
            base("tuple index does not allow setting cbVarSegMac", JET_err.IndexTuplesVarSegMacNotAllowed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesVarSegMacNotAllowedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesVarSegMacNotAllowedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesInvalidLimits exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesInvalidLimitsException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesInvalidLimitsException class.
        /// </summary>
        public EsentIndexTuplesInvalidLimitsException() :
            base("invalid min/max tuple length or max characters to index specified", JET_err.IndexTuplesInvalidLimits)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesInvalidLimitsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesInvalidLimitsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesCannotRetrieveFromIndex exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesCannotRetrieveFromIndexException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesCannotRetrieveFromIndexException class.
        /// </summary>
        public EsentIndexTuplesCannotRetrieveFromIndexException() :
            base("cannot call RetrieveColumn() with RetrieveFromIndex on a tuple index", JET_err.IndexTuplesCannotRetrieveFromIndex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesCannotRetrieveFromIndexException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesCannotRetrieveFromIndexException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.IndexTuplesKeyTooSmall exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentIndexTuplesKeyTooSmallException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesKeyTooSmallException class.
        /// </summary>
        public EsentIndexTuplesKeyTooSmallException() :
            base("specified key does not meet minimum tuple length", JET_err.IndexTuplesKeyTooSmall)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentIndexTuplesKeyTooSmallException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentIndexTuplesKeyTooSmallException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnLong exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnLongException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnLongException class.
        /// </summary>
        public EsentColumnLongException() :
            base("Column value is long", JET_err.ColumnLong)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnLongException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnLongException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnNoChunk exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnNoChunkException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnNoChunkException class.
        /// </summary>
        public EsentColumnNoChunkException() :
            base("No such chunk in long value", JET_err.ColumnNoChunk)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnNoChunkException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnNoChunkException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnDoesNotFit exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnDoesNotFitException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnDoesNotFitException class.
        /// </summary>
        public EsentColumnDoesNotFitException() :
            base("Field will not fit in record", JET_err.ColumnDoesNotFit)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnDoesNotFitException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnDoesNotFitException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NullInvalid exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNullInvalidException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNullInvalidException class.
        /// </summary>
        public EsentNullInvalidException() :
            base("Null not valid", JET_err.NullInvalid)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNullInvalidException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNullInvalidException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnIndexed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnIndexedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnIndexedException class.
        /// </summary>
        public EsentColumnIndexedException() :
            base("Column indexed, cannot delete", JET_err.ColumnIndexed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnIndexedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnIndexedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnTooBig exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnTooBigException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnTooBigException class.
        /// </summary>
        public EsentColumnTooBigException() :
            base("Field length is greater than maximum", JET_err.ColumnTooBig)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnTooBigException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnTooBigException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnNotFoundException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnNotFoundException class.
        /// </summary>
        public EsentColumnNotFoundException() :
            base("No such column", JET_err.ColumnNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnDuplicateException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnDuplicateException class.
        /// </summary>
        public EsentColumnDuplicateException() :
            base("Field is already defined", JET_err.ColumnDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MultiValuedColumnMustBeTagged exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMultiValuedColumnMustBeTaggedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedColumnMustBeTaggedException class.
        /// </summary>
        public EsentMultiValuedColumnMustBeTaggedException() :
            base("Attempted to create a multi-valued column, but column was not Tagged", JET_err.MultiValuedColumnMustBeTagged)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedColumnMustBeTaggedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMultiValuedColumnMustBeTaggedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnRedundant exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnRedundantException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnRedundantException class.
        /// </summary>
        public EsentColumnRedundantException() :
            base("Second autoincrement or version column", JET_err.ColumnRedundant)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnRedundantException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnRedundantException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidColumnType exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidColumnTypeException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidColumnTypeException class.
        /// </summary>
        public EsentInvalidColumnTypeException() :
            base("Invalid column data type", JET_err.InvalidColumnType)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidColumnTypeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidColumnTypeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TaggedNotNULL exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTaggedNotNULLException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTaggedNotNULLException class.
        /// </summary>
        public EsentTaggedNotNULLException() :
            base("No non-NULL tagged columns", JET_err.TaggedNotNULL)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTaggedNotNULLException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTaggedNotNULLException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NoCurrentIndex exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNoCurrentIndexException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNoCurrentIndexException class.
        /// </summary>
        public EsentNoCurrentIndexException() :
            base("Invalid w/o a current index", JET_err.NoCurrentIndex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNoCurrentIndexException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNoCurrentIndexException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.KeyIsMade exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentKeyIsMadeException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentKeyIsMadeException class.
        /// </summary>
        public EsentKeyIsMadeException() :
            base("The key is completely made", JET_err.KeyIsMade)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentKeyIsMadeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentKeyIsMadeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadColumnId exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadColumnIdException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadColumnIdException class.
        /// </summary>
        public EsentBadColumnIdException() :
            base("Column Id Incorrect", JET_err.BadColumnId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadColumnIdException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadColumnIdException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.BadItagSequence exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentBadItagSequenceException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentBadItagSequenceException class.
        /// </summary>
        public EsentBadItagSequenceException() :
            base("Bad itagSequence for tagged column", JET_err.BadItagSequence)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentBadItagSequenceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentBadItagSequenceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnInRelationship exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnInRelationshipException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnInRelationshipException class.
        /// </summary>
        public EsentColumnInRelationshipException() :
            base("Cannot delete, column participates in relationship", JET_err.ColumnInRelationship)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnInRelationshipException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnInRelationshipException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CannotBeTagged exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCannotBeTaggedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCannotBeTaggedException class.
        /// </summary>
        public EsentCannotBeTaggedException() :
            base("AutoIncrement and Version cannot be tagged", JET_err.CannotBeTagged)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCannotBeTaggedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCannotBeTaggedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DefaultValueTooBig exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDefaultValueTooBigException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDefaultValueTooBigException class.
        /// </summary>
        public EsentDefaultValueTooBigException() :
            base("Default value exceeds maximum size", JET_err.DefaultValueTooBig)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDefaultValueTooBigException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDefaultValueTooBigException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MultiValuedDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMultiValuedDuplicateException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedDuplicateException class.
        /// </summary>
        public EsentMultiValuedDuplicateException() :
            base("Duplicate detected on a unique multi-valued column", JET_err.MultiValuedDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMultiValuedDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LVCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLVCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLVCorruptedException class.
        /// </summary>
        public EsentLVCorruptedException() :
            base("Corruption encountered in long-value tree", JET_err.LVCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLVCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLVCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.MultiValuedDuplicateAfterTruncation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentMultiValuedDuplicateAfterTruncationException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedDuplicateAfterTruncationException class.
        /// </summary>
        public EsentMultiValuedDuplicateAfterTruncationException() :
            base("Duplicate detected on a unique multi-valued column after data was normalized, and normalizing truncated the data before comparison", JET_err.MultiValuedDuplicateAfterTruncation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentMultiValuedDuplicateAfterTruncationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentMultiValuedDuplicateAfterTruncationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DerivedColumnCorruption exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDerivedColumnCorruptionException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDerivedColumnCorruptionException class.
        /// </summary>
        public EsentDerivedColumnCorruptionException() :
            base("Invalid column in derived table", JET_err.DerivedColumnCorruption)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDerivedColumnCorruptionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDerivedColumnCorruptionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidPlaceholderColumn exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidPlaceholderColumnException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidPlaceholderColumnException class.
        /// </summary>
        public EsentInvalidPlaceholderColumnException() :
            base("Tried to convert column to a primary index placeholder, but column doesn't meet necessary criteria", JET_err.InvalidPlaceholderColumn)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidPlaceholderColumnException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidPlaceholderColumnException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.ColumnCannotBeCompressed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentColumnCannotBeCompressedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnCannotBeCompressedException class.
        /// </summary>
        public EsentColumnCannotBeCompressedException() :
            base("Only JET_coltypLongText and JET_coltypLongBinary columns can be compressed", JET_err.ColumnCannotBeCompressed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnCannotBeCompressedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentColumnCannotBeCompressedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordNotFoundException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordNotFoundException class.
        /// </summary>
        public EsentRecordNotFoundException() :
            base("The key was not found", JET_err.RecordNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordNoCopy exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordNoCopyException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordNoCopyException class.
        /// </summary>
        public EsentRecordNoCopyException() :
            base("No working buffer", JET_err.RecordNoCopy)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordNoCopyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordNoCopyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.NoCurrentRecord exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentNoCurrentRecordException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentNoCurrentRecordException class.
        /// </summary>
        public EsentNoCurrentRecordException() :
            base("Currency not on a record", JET_err.NoCurrentRecord)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentNoCurrentRecordException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentNoCurrentRecordException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordPrimaryChanged exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordPrimaryChangedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordPrimaryChangedException class.
        /// </summary>
        public EsentRecordPrimaryChangedException() :
            base("Primary key may not change", JET_err.RecordPrimaryChanged)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordPrimaryChangedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordPrimaryChangedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.KeyDuplicate exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentKeyDuplicateException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentKeyDuplicateException class.
        /// </summary>
        public EsentKeyDuplicateException() :
            base("Illegal duplicate key", JET_err.KeyDuplicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentKeyDuplicateException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentKeyDuplicateException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.AlreadyPrepared exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentAlreadyPreparedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentAlreadyPreparedException class.
        /// </summary>
        public EsentAlreadyPreparedException() :
            base("Attempted to update record when record update was already in progress", JET_err.AlreadyPrepared)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentAlreadyPreparedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentAlreadyPreparedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.KeyNotMade exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentKeyNotMadeException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentKeyNotMadeException class.
        /// </summary>
        public EsentKeyNotMadeException() :
            base("No call to JetMakeKey", JET_err.KeyNotMade)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentKeyNotMadeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentKeyNotMadeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.UpdateNotPrepared exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentUpdateNotPreparedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUpdateNotPreparedException class.
        /// </summary>
        public EsentUpdateNotPreparedException() :
            base("No call to JetPrepareUpdate", JET_err.UpdateNotPrepared)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUpdateNotPreparedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentUpdateNotPreparedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DataHasChanged exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDataHasChangedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDataHasChangedException class.
        /// </summary>
        public EsentDataHasChangedException() :
            base("Data has changed, operation aborted", JET_err.DataHasChanged)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDataHasChangedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDataHasChangedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LanguageNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLanguageNotSupportedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLanguageNotSupportedException class.
        /// </summary>
        public EsentLanguageNotSupportedException() :
            base("Windows installation does not support language", JET_err.LanguageNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLanguageNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLanguageNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DecompressionFailed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDecompressionFailedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDecompressionFailedException class.
        /// </summary>
        public EsentDecompressionFailedException() :
            base("Internal error: data could not be decompressed", JET_err.DecompressionFailed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDecompressionFailedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDecompressionFailedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.UpdateMustVersion exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentUpdateMustVersionException : EsentErrorException
    {
        /// <summary>
        /// Initializes a new instance of the EsentUpdateMustVersionException class.
        /// </summary>
        public EsentUpdateMustVersionException() :
            base("No version updates only for uncommitted tables", JET_err.UpdateMustVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentUpdateMustVersionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentUpdateMustVersionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManySorts exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManySortsException : EsentMemoryException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManySortsException class.
        /// </summary>
        public EsentTooManySortsException() :
            base("Too many sort processes", JET_err.TooManySorts)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManySortsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManySortsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidOnSort exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidOnSortException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidOnSortException class.
        /// </summary>
        public EsentInvalidOnSortException() :
            base("Invalid operation on Sort", JET_err.InvalidOnSort)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidOnSortException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidOnSortException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TempFileOpenError exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTempFileOpenErrorException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTempFileOpenErrorException class.
        /// </summary>
        public EsentTempFileOpenErrorException() :
            base("Temp file could not be opened", JET_err.TempFileOpenError)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTempFileOpenErrorException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTempFileOpenErrorException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyAttachedDatabases exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyAttachedDatabasesException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyAttachedDatabasesException class.
        /// </summary>
        public EsentTooManyAttachedDatabasesException() :
            base("Too many open databases", JET_err.TooManyAttachedDatabases)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyAttachedDatabasesException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyAttachedDatabasesException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DiskFull exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDiskFullException : EsentDiskException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDiskFullException class.
        /// </summary>
        public EsentDiskFullException() :
            base("No space left on disk", JET_err.DiskFull)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDiskFullException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDiskFullException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.PermissionDenied exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentPermissionDeniedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentPermissionDeniedException class.
        /// </summary>
        public EsentPermissionDeniedException() :
            base("Permission denied", JET_err.PermissionDenied)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentPermissionDeniedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentPermissionDeniedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileNotFoundException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileNotFoundException class.
        /// </summary>
        public EsentFileNotFoundException() :
            base("File not found", JET_err.FileNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileInvalidType exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileInvalidTypeException : EsentInconsistentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileInvalidTypeException class.
        /// </summary>
        public EsentFileInvalidTypeException() :
            base("Invalid file type", JET_err.FileInvalidType)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileInvalidTypeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileInvalidTypeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.AfterInitialization exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentAfterInitializationException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentAfterInitializationException class.
        /// </summary>
        public EsentAfterInitializationException() :
            base("Cannot Restore after init.", JET_err.AfterInitialization)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentAfterInitializationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentAfterInitializationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LogCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLogCorruptedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLogCorruptedException class.
        /// </summary>
        public EsentLogCorruptedException() :
            base("Logs could not be interpreted", JET_err.LogCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLogCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLogCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidOperation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidOperationException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidOperationException class.
        /// </summary>
        public EsentInvalidOperationException() :
            base("Invalid operation", JET_err.InvalidOperation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidOperationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidOperationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.AccessDenied exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentAccessDeniedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentAccessDeniedException class.
        /// </summary>
        public EsentAccessDeniedException() :
            base("Access denied", JET_err.AccessDenied)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentAccessDeniedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentAccessDeniedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManySplits exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManySplitsException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManySplitsException class.
        /// </summary>
        public EsentTooManySplitsException() :
            base("Infinite split", JET_err.TooManySplits)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManySplitsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManySplitsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SessionSharingViolation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSessionSharingViolationException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSessionSharingViolationException class.
        /// </summary>
        public EsentSessionSharingViolationException() :
            base("Multiple threads are using the same session", JET_err.SessionSharingViolation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSessionSharingViolationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSessionSharingViolationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.EntryPointNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentEntryPointNotFoundException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentEntryPointNotFoundException class.
        /// </summary>
        public EsentEntryPointNotFoundException() :
            base("An entry point in a DLL we require could not be found", JET_err.EntryPointNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentEntryPointNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentEntryPointNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SessionContextAlreadySet exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSessionContextAlreadySetException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSessionContextAlreadySetException class.
        /// </summary>
        public EsentSessionContextAlreadySetException() :
            base("Specified session already has a session context set", JET_err.SessionContextAlreadySet)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSessionContextAlreadySetException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSessionContextAlreadySetException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SessionContextNotSetByThisThread exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSessionContextNotSetByThisThreadException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSessionContextNotSetByThisThreadException class.
        /// </summary>
        public EsentSessionContextNotSetByThisThreadException() :
            base("Tried to reset session context, but current thread did not orignally set the session context", JET_err.SessionContextNotSetByThisThread)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSessionContextNotSetByThisThreadException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSessionContextNotSetByThisThreadException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SessionInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSessionInUseException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSessionInUseException class.
        /// </summary>
        public EsentSessionInUseException() :
            base("Tried to terminate session in use", JET_err.SessionInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSessionInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSessionInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RecordFormatConversionFailed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRecordFormatConversionFailedException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRecordFormatConversionFailedException class.
        /// </summary>
        public EsentRecordFormatConversionFailedException() :
            base("Internal error during dynamic record format conversion", JET_err.RecordFormatConversionFailed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRecordFormatConversionFailedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRecordFormatConversionFailedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OneDatabasePerSession exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOneDatabasePerSessionException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOneDatabasePerSessionException class.
        /// </summary>
        public EsentOneDatabasePerSessionException() :
            base("Just one open user database per session is allowed (JET_paramOneDatabasePerSession)", JET_err.OneDatabasePerSession)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOneDatabasePerSessionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOneDatabasePerSessionException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.RollbackError exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentRollbackErrorException : EsentFatalException
    {
        /// <summary>
        /// Initializes a new instance of the EsentRollbackErrorException class.
        /// </summary>
        public EsentRollbackErrorException() :
            base("error during rollback", JET_err.RollbackError)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentRollbackErrorException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentRollbackErrorException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.DatabaseAlreadyRunningMaintenance exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentDatabaseAlreadyRunningMaintenanceException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentDatabaseAlreadyRunningMaintenanceException class.
        /// </summary>
        public EsentDatabaseAlreadyRunningMaintenanceException() :
            base("The operation did not complete successfully because the database is already running maintenance on specified database", JET_err.DatabaseAlreadyRunningMaintenance)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentDatabaseAlreadyRunningMaintenanceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentDatabaseAlreadyRunningMaintenanceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CallbackFailed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCallbackFailedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCallbackFailedException class.
        /// </summary>
        public EsentCallbackFailedException() :
            base("A callback failed", JET_err.CallbackFailed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCallbackFailedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCallbackFailedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.CallbackNotResolved exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentCallbackNotResolvedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentCallbackNotResolvedException class.
        /// </summary>
        public EsentCallbackNotResolvedException() :
            base("A callback function could not be found", JET_err.CallbackNotResolved)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentCallbackNotResolvedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentCallbackNotResolvedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SpaceHintsInvalid exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSpaceHintsInvalidException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSpaceHintsInvalidException class.
        /// </summary>
        public EsentSpaceHintsInvalidException() :
            base("An element of the JET space hints structure was not correct or actionable.", JET_err.SpaceHintsInvalid)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSpaceHintsInvalidException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSpaceHintsInvalidException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVSpaceCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVSpaceCorruptedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVSpaceCorruptedException class.
        /// </summary>
        public EsentSLVSpaceCorruptedException() :
            base("Corruption encountered in space manager of streaming file", JET_err.SLVSpaceCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVSpaceCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVSpaceCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVCorruptedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVCorruptedException class.
        /// </summary>
        public EsentSLVCorruptedException() :
            base("Corruption encountered in streaming file", JET_err.SLVCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVColumnDefaultValueNotAllowed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVColumnDefaultValueNotAllowedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVColumnDefaultValueNotAllowedException class.
        /// </summary>
        public EsentSLVColumnDefaultValueNotAllowedException() :
            base("SLV columns cannot have a default value", JET_err.SLVColumnDefaultValueNotAllowed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVColumnDefaultValueNotAllowedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVColumnDefaultValueNotAllowedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVStreamingFileMissing exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVStreamingFileMissingException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileMissingException class.
        /// </summary>
        public EsentSLVStreamingFileMissingException() :
            base("Cannot find streaming file associated with this database", JET_err.SLVStreamingFileMissing)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileMissingException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVStreamingFileMissingException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVDatabaseMissing exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVDatabaseMissingException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVDatabaseMissingException class.
        /// </summary>
        public EsentSLVDatabaseMissingException() :
            base("Streaming file exists, but database to which it belongs is missing", JET_err.SLVDatabaseMissing)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVDatabaseMissingException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVDatabaseMissingException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVStreamingFileAlreadyExists exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVStreamingFileAlreadyExistsException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileAlreadyExistsException class.
        /// </summary>
        public EsentSLVStreamingFileAlreadyExistsException() :
            base("Tried to create a streaming file when one already exists or is already recorded in the catalog", JET_err.SLVStreamingFileAlreadyExists)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileAlreadyExistsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVStreamingFileAlreadyExistsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVInvalidPath exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVInvalidPathException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVInvalidPathException class.
        /// </summary>
        public EsentSLVInvalidPathException() :
            base("Specified path to a streaming file is invalid", JET_err.SLVInvalidPath)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVInvalidPathException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVInvalidPathException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVStreamingFileNotCreated exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVStreamingFileNotCreatedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileNotCreatedException class.
        /// </summary>
        public EsentSLVStreamingFileNotCreatedException() :
            base("Tried to perform an SLV operation but streaming file was never created", JET_err.SLVStreamingFileNotCreated)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileNotCreatedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVStreamingFileNotCreatedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVStreamingFileReadOnly exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVStreamingFileReadOnlyException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileReadOnlyException class.
        /// </summary>
        public EsentSLVStreamingFileReadOnlyException() :
            base("Attach a readonly streaming file for read/write operations", JET_err.SLVStreamingFileReadOnly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileReadOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVStreamingFileReadOnlyException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVHeaderBadChecksum exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVHeaderBadChecksumException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVHeaderBadChecksumException class.
        /// </summary>
        public EsentSLVHeaderBadChecksumException() :
            base("SLV file header failed checksum verification", JET_err.SLVHeaderBadChecksum)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVHeaderBadChecksumException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVHeaderBadChecksumException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVHeaderCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVHeaderCorruptedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVHeaderCorruptedException class.
        /// </summary>
        public EsentSLVHeaderCorruptedException() :
            base("SLV file header contains invalid information", JET_err.SLVHeaderCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVHeaderCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVHeaderCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVPagesNotFree exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVPagesNotFreeException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotFreeException class.
        /// </summary>
        public EsentSLVPagesNotFreeException() :
            base("Tried to move pages from the Free state when they were not in that state", JET_err.SLVPagesNotFree)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotFreeException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVPagesNotFreeException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVPagesNotReserved exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVPagesNotReservedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotReservedException class.
        /// </summary>
        public EsentSLVPagesNotReservedException() :
            base("Tried to move pages from the Reserved state when they were not in that state", JET_err.SLVPagesNotReserved)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotReservedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVPagesNotReservedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVPagesNotCommitted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVPagesNotCommittedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotCommittedException class.
        /// </summary>
        public EsentSLVPagesNotCommittedException() :
            base("Tried to move pages from the Committed state when they were not in that state", JET_err.SLVPagesNotCommitted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotCommittedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVPagesNotCommittedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVPagesNotDeleted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVPagesNotDeletedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotDeletedException class.
        /// </summary>
        public EsentSLVPagesNotDeletedException() :
            base("Tried to move pages from the Deleted state when they were not in that state", JET_err.SLVPagesNotDeleted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVPagesNotDeletedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVPagesNotDeletedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVSpaceWriteConflict exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVSpaceWriteConflictException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVSpaceWriteConflictException class.
        /// </summary>
        public EsentSLVSpaceWriteConflictException() :
            base("Unexpected conflict detected trying to write-latch SLV space pages", JET_err.SLVSpaceWriteConflict)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVSpaceWriteConflictException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVSpaceWriteConflictException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVRootStillOpen exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVRootStillOpenException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVRootStillOpenException class.
        /// </summary>
        public EsentSLVRootStillOpenException() :
            base("The database can not be created/attached because its corresponding SLV Root is still open by another process.", JET_err.SLVRootStillOpen)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVRootStillOpenException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVRootStillOpenException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVProviderNotLoaded exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVProviderNotLoadedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVProviderNotLoadedException class.
        /// </summary>
        public EsentSLVProviderNotLoadedException() :
            base("The database can not be created/attached because the SLV Provider has not been loaded.", JET_err.SLVProviderNotLoaded)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVProviderNotLoadedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVProviderNotLoadedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVEAListCorrupt exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVEAListCorruptException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVEAListCorruptException class.
        /// </summary>
        public EsentSLVEAListCorruptException() :
            base("The specified SLV EA List is corrupted.", JET_err.SLVEAListCorrupt)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVEAListCorruptException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVEAListCorruptException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVRootNotSpecified exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVRootNotSpecifiedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVRootNotSpecifiedException class.
        /// </summary>
        public EsentSLVRootNotSpecifiedException() :
            base("The database cannot be created/attached because the SLV Root Name was omitted", JET_err.SLVRootNotSpecified)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVRootNotSpecifiedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVRootNotSpecifiedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVRootPathInvalid exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVRootPathInvalidException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVRootPathInvalidException class.
        /// </summary>
        public EsentSLVRootPathInvalidException() :
            base("The specified SLV Root path was invalid.", JET_err.SLVRootPathInvalid)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVRootPathInvalidException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVRootPathInvalidException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVEAListZeroAllocation exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVEAListZeroAllocationException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVEAListZeroAllocationException class.
        /// </summary>
        public EsentSLVEAListZeroAllocationException() :
            base("The specified SLV EA List has no allocated space.", JET_err.SLVEAListZeroAllocation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVEAListZeroAllocationException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVEAListZeroAllocationException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVColumnCannotDelete exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVColumnCannotDeleteException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVColumnCannotDeleteException class.
        /// </summary>
        public EsentSLVColumnCannotDeleteException() :
            base("Deletion of SLV columns is not currently supported.", JET_err.SLVColumnCannotDelete)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVColumnCannotDeleteException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVColumnCannotDeleteException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVOwnerMapAlreadyExists exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVOwnerMapAlreadyExistsException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVOwnerMapAlreadyExistsException class.
        /// </summary>
        public EsentSLVOwnerMapAlreadyExistsException() :
            base("Tried to create a new catalog entry for SLV Ownership Map when one already exists", JET_err.SLVOwnerMapAlreadyExists)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVOwnerMapAlreadyExistsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVOwnerMapAlreadyExistsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVOwnerMapCorrupted exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVOwnerMapCorruptedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVOwnerMapCorruptedException class.
        /// </summary>
        public EsentSLVOwnerMapCorruptedException() :
            base("Corruption encountered in SLV Ownership Map", JET_err.SLVOwnerMapCorrupted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVOwnerMapCorruptedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVOwnerMapCorruptedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVOwnerMapPageNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVOwnerMapPageNotFoundException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVOwnerMapPageNotFoundException class.
        /// </summary>
        public EsentSLVOwnerMapPageNotFoundException() :
            base("Corruption encountered in SLV Ownership Map", JET_err.SLVOwnerMapPageNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVOwnerMapPageNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVOwnerMapPageNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileStale exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileStaleException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileStaleException class.
        /// </summary>
        public EsentSLVFileStaleException() :
            base("The specified SLV File handle belongs to a SLV Root that no longer exists.", JET_err.SLVFileStale)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileStaleException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileStaleException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileInUseException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileInUseException class.
        /// </summary>
        public EsentSLVFileInUseException() :
            base("The specified SLV File is currently in use", JET_err.SLVFileInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVStreamingFileInUse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVStreamingFileInUseException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileInUseException class.
        /// </summary>
        public EsentSLVStreamingFileInUseException() :
            base("The specified streaming file is currently in use", JET_err.SLVStreamingFileInUse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileInUseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVStreamingFileInUseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileIO exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileIOException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileIOException class.
        /// </summary>
        public EsentSLVFileIOException() :
            base("An I/O error occurred while accessing an SLV File (general read / write failure)", JET_err.SLVFileIO)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileIOException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileIOException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVStreamingFileFull exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVStreamingFileFullException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileFullException class.
        /// </summary>
        public EsentSLVStreamingFileFullException() :
            base("No space left in the streaming file", JET_err.SLVStreamingFileFull)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVStreamingFileFullException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVStreamingFileFullException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileInvalidPath exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileInvalidPathException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileInvalidPathException class.
        /// </summary>
        public EsentSLVFileInvalidPathException() :
            base("Specified path to a SLV File was invalid", JET_err.SLVFileInvalidPath)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileInvalidPathException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileInvalidPathException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileAccessDenied exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileAccessDeniedException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileAccessDeniedException class.
        /// </summary>
        public EsentSLVFileAccessDeniedException() :
            base("Cannot access SLV File, the SLV File is locked or is in use", JET_err.SLVFileAccessDenied)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileAccessDeniedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileAccessDeniedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileNotFound exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileNotFoundException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileNotFoundException class.
        /// </summary>
        public EsentSLVFileNotFoundException() :
            base("The specified SLV File was not found", JET_err.SLVFileNotFound)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileNotFoundException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVFileUnknown exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVFileUnknownException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVFileUnknownException class.
        /// </summary>
        public EsentSLVFileUnknownException() :
            base("An unknown error occurred while accessing an SLV File", JET_err.SLVFileUnknown)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVFileUnknownException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVFileUnknownException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVEAListTooBig exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVEAListTooBigException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVEAListTooBigException class.
        /// </summary>
        public EsentSLVEAListTooBigException() :
            base("The specified SLV EA List could not be returned because it is too large to fit in the standard EA format.  Retrieve the SLV File as a file handle instead.", JET_err.SLVEAListTooBig)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVEAListTooBigException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVEAListTooBigException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVProviderVersionMismatch exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVProviderVersionMismatchException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVProviderVersionMismatchException class.
        /// </summary>
        public EsentSLVProviderVersionMismatchException() :
            base("The loaded SLV Provider's version does not match the database engine's version.", JET_err.SLVProviderVersionMismatch)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVProviderVersionMismatchException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVProviderVersionMismatchException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.SLVBufferTooSmall exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentSLVBufferTooSmallException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSLVBufferTooSmallException class.
        /// </summary>
        public EsentSLVBufferTooSmallException() :
            base("Buffer allocated for SLV data or meta-data was too small", JET_err.SLVBufferTooSmall)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSLVBufferTooSmallException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentSLVBufferTooSmallException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OSSnapshotInvalidSequence exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOSSnapshotInvalidSequenceException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotInvalidSequenceException class.
        /// </summary>
        public EsentOSSnapshotInvalidSequenceException() :
            base("OS Shadow copy API used in an invalid sequence", JET_err.OSSnapshotInvalidSequence)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotInvalidSequenceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOSSnapshotInvalidSequenceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OSSnapshotTimeOut exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOSSnapshotTimeOutException : EsentOperationException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotTimeOutException class.
        /// </summary>
        public EsentOSSnapshotTimeOutException() :
            base("OS Shadow copy ended with time-out", JET_err.OSSnapshotTimeOut)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotTimeOutException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOSSnapshotTimeOutException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OSSnapshotNotAllowed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOSSnapshotNotAllowedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotNotAllowedException class.
        /// </summary>
        public EsentOSSnapshotNotAllowedException() :
            base("OS Shadow copy not allowed (backup or recovery in progress)", JET_err.OSSnapshotNotAllowed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotNotAllowedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOSSnapshotNotAllowedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.OSSnapshotInvalidSnapId exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentOSSnapshotInvalidSnapIdException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotInvalidSnapIdException class.
        /// </summary>
        public EsentOSSnapshotInvalidSnapIdException() :
            base("invalid JET_OSSNAPID", JET_err.OSSnapshotInvalidSnapId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentOSSnapshotInvalidSnapIdException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentOSSnapshotInvalidSnapIdException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TooManyTestInjections exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTooManyTestInjectionsException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTooManyTestInjectionsException class.
        /// </summary>
        public EsentTooManyTestInjectionsException() :
            base("Internal test injection limit hit", JET_err.TooManyTestInjections)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTooManyTestInjectionsException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTooManyTestInjectionsException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.TestInjectionNotSupported exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentTestInjectionNotSupportedException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentTestInjectionNotSupportedException class.
        /// </summary>
        public EsentTestInjectionNotSupportedException() :
            base("Test injection not supported", JET_err.TestInjectionNotSupported)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentTestInjectionNotSupportedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentTestInjectionNotSupportedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.InvalidLogDataSequence exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentInvalidLogDataSequenceException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidLogDataSequenceException class.
        /// </summary>
        public EsentInvalidLogDataSequenceException() :
            base("Some how the log data provided got out of sequence with the current state of the instance", JET_err.InvalidLogDataSequence)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidLogDataSequenceException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentInvalidLogDataSequenceException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LSCallbackNotSpecified exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLSCallbackNotSpecifiedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLSCallbackNotSpecifiedException class.
        /// </summary>
        public EsentLSCallbackNotSpecifiedException() :
            base("Attempted to use Local Storage without a callback function being specified", JET_err.LSCallbackNotSpecified)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLSCallbackNotSpecifiedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLSCallbackNotSpecifiedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LSAlreadySet exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLSAlreadySetException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLSAlreadySetException class.
        /// </summary>
        public EsentLSAlreadySetException() :
            base("Attempted to set Local Storage for an object which already had it set", JET_err.LSAlreadySet)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLSAlreadySetException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLSAlreadySetException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.LSNotSet exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentLSNotSetException : EsentStateException
    {
        /// <summary>
        /// Initializes a new instance of the EsentLSNotSetException class.
        /// </summary>
        public EsentLSNotSetException() :
            base("Attempted to retrieve Local Storage from an object which didn't have it set", JET_err.LSNotSet)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentLSNotSetException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentLSNotSetException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileIOSparse exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileIOSparseException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileIOSparseException class.
        /// </summary>
        public EsentFileIOSparseException() :
            base("an I/O was issued to a location that was sparse", JET_err.FileIOSparse)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileIOSparseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileIOSparseException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileIOBeyondEOF exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileIOBeyondEOFException : EsentCorruptionException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileIOBeyondEOFException class.
        /// </summary>
        public EsentFileIOBeyondEOFException() :
            base("a read was issued to a location beyond EOF (writes will expand the file)", JET_err.FileIOBeyondEOF)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileIOBeyondEOFException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileIOBeyondEOFException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileIOAbort exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileIOAbortException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileIOAbortException class.
        /// </summary>
        public EsentFileIOAbortException() :
            base("instructs the JET_ABORTRETRYFAILCALLBACK caller to abort the specified I/O", JET_err.FileIOAbort)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileIOAbortException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileIOAbortException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileIORetry exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileIORetryException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileIORetryException class.
        /// </summary>
        public EsentFileIORetryException() :
            base("instructs the JET_ABORTRETRYFAILCALLBACK caller to retry the specified I/O", JET_err.FileIORetry)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileIORetryException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileIORetryException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileIOFail exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileIOFailException : EsentObsoleteException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileIOFailException class.
        /// </summary>
        public EsentFileIOFailException() :
            base("instructs the JET_ABORTRETRYFAILCALLBACK caller to fail the specified I/O", JET_err.FileIOFail)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileIOFailException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileIOFailException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Base class for JET_err.FileCompressed exceptions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.MaintainabilityRules",
        "SA1402:FileMayOnlyContainASingleClass",
        Justification = "Auto-generated code.")]
    [Serializable]
    public sealed class EsentFileCompressedException : EsentUsageException
    {
        /// <summary>
        /// Initializes a new instance of the EsentFileCompressedException class.
        /// </summary>
        public EsentFileCompressedException() :
            base("read/write access is not supported on compressed files", JET_err.FileCompressed)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentFileCompressedException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        private EsentFileCompressedException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
        }
    }

    /// <summary>
    /// Method to generate an EsentErrorException from an error code.
    /// </summary>
    internal static class EsentExceptionHelper
    {
        /// <summary>
        /// Create an EsentErrorException from an error code.
        /// </summary>
        /// <param name="err">The error code.</param>
        /// <returns>An EsentErrorException for the error code.</returns>
        public static EsentErrorException JetErrToException(JET_err err)
        {
        switch (err)
            {
            case JET_err.RfsFailure:
                return new EsentRfsFailureException();
            case JET_err.RfsNotArmed:
                return new EsentRfsNotArmedException();
            case JET_err.FileClose:
                return new EsentFileCloseException();
            case JET_err.OutOfThreads:
                return new EsentOutOfThreadsException();
            case JET_err.TooManyIO:
                return new EsentTooManyIOException();
            case JET_err.TaskDropped:
                return new EsentTaskDroppedException();
            case JET_err.InternalError:
                return new EsentInternalErrorException();
            case JET_err.DisabledFunctionality:
                return new EsentDisabledFunctionalityException();
            case JET_err.DatabaseBufferDependenciesCorrupted:
                return new EsentDatabaseBufferDependenciesCorruptedException();
            case JET_err.PreviousVersion:
                return new EsentPreviousVersionException();
            case JET_err.PageBoundary:
                return new EsentPageBoundaryException();
            case JET_err.KeyBoundary:
                return new EsentKeyBoundaryException();
            case JET_err.BadPageLink:
                return new EsentBadPageLinkException();
            case JET_err.BadBookmark:
                return new EsentBadBookmarkException();
            case JET_err.NTSystemCallFailed:
                return new EsentNTSystemCallFailedException();
            case JET_err.BadParentPageLink:
                return new EsentBadParentPageLinkException();
            case JET_err.SPAvailExtCacheOutOfSync:
                return new EsentSPAvailExtCacheOutOfSyncException();
            case JET_err.SPAvailExtCorrupted:
                return new EsentSPAvailExtCorruptedException();
            case JET_err.SPAvailExtCacheOutOfMemory:
                return new EsentSPAvailExtCacheOutOfMemoryException();
            case JET_err.SPOwnExtCorrupted:
                return new EsentSPOwnExtCorruptedException();
            case JET_err.DbTimeCorrupted:
                return new EsentDbTimeCorruptedException();
            case JET_err.KeyTruncated:
                return new EsentKeyTruncatedException();
            case JET_err.DatabaseLeakInSpace:
                return new EsentDatabaseLeakInSpaceException();
            case JET_err.KeyTooBig:
                return new EsentKeyTooBigException();
            case JET_err.CannotSeparateIntrinsicLV:
                return new EsentCannotSeparateIntrinsicLVException();
            case JET_err.SeparatedLongValue:
                return new EsentSeparatedLongValueException();
            case JET_err.InvalidLoggedOperation:
                return new EsentInvalidLoggedOperationException();
            case JET_err.LogFileCorrupt:
                return new EsentLogFileCorruptException();
            case JET_err.NoBackupDirectory:
                return new EsentNoBackupDirectoryException();
            case JET_err.BackupDirectoryNotEmpty:
                return new EsentBackupDirectoryNotEmptyException();
            case JET_err.BackupInProgress:
                return new EsentBackupInProgressException();
            case JET_err.RestoreInProgress:
                return new EsentRestoreInProgressException();
            case JET_err.MissingPreviousLogFile:
                return new EsentMissingPreviousLogFileException();
            case JET_err.LogWriteFail:
                return new EsentLogWriteFailException();
            case JET_err.LogDisabledDueToRecoveryFailure:
                return new EsentLogDisabledDueToRecoveryFailureException();
            case JET_err.CannotLogDuringRecoveryRedo:
                return new EsentCannotLogDuringRecoveryRedoException();
            case JET_err.LogGenerationMismatch:
                return new EsentLogGenerationMismatchException();
            case JET_err.BadLogVersion:
                return new EsentBadLogVersionException();
            case JET_err.InvalidLogSequence:
                return new EsentInvalidLogSequenceException();
            case JET_err.LoggingDisabled:
                return new EsentLoggingDisabledException();
            case JET_err.LogBufferTooSmall:
                return new EsentLogBufferTooSmallException();
            case JET_err.LogSequenceEnd:
                return new EsentLogSequenceEndException();
            case JET_err.NoBackup:
                return new EsentNoBackupException();
            case JET_err.InvalidBackupSequence:
                return new EsentInvalidBackupSequenceException();
            case JET_err.BackupNotAllowedYet:
                return new EsentBackupNotAllowedYetException();
            case JET_err.DeleteBackupFileFail:
                return new EsentDeleteBackupFileFailException();
            case JET_err.MakeBackupDirectoryFail:
                return new EsentMakeBackupDirectoryFailException();
            case JET_err.InvalidBackup:
                return new EsentInvalidBackupException();
            case JET_err.RecoveredWithErrors:
                return new EsentRecoveredWithErrorsException();
            case JET_err.MissingLogFile:
                return new EsentMissingLogFileException();
            case JET_err.LogDiskFull:
                return new EsentLogDiskFullException();
            case JET_err.BadLogSignature:
                return new EsentBadLogSignatureException();
            case JET_err.BadDbSignature:
                return new EsentBadDbSignatureException();
            case JET_err.BadCheckpointSignature:
                return new EsentBadCheckpointSignatureException();
            case JET_err.CheckpointCorrupt:
                return new EsentCheckpointCorruptException();
            case JET_err.MissingPatchPage:
                return new EsentMissingPatchPageException();
            case JET_err.BadPatchPage:
                return new EsentBadPatchPageException();
            case JET_err.RedoAbruptEnded:
                return new EsentRedoAbruptEndedException();
            case JET_err.BadSLVSignature:
                return new EsentBadSLVSignatureException();
            case JET_err.PatchFileMissing:
                return new EsentPatchFileMissingException();
            case JET_err.DatabaseLogSetMismatch:
                return new EsentDatabaseLogSetMismatchException();
            case JET_err.DatabaseStreamingFileMismatch:
                return new EsentDatabaseStreamingFileMismatchException();
            case JET_err.LogFileSizeMismatch:
                return new EsentLogFileSizeMismatchException();
            case JET_err.CheckpointFileNotFound:
                return new EsentCheckpointFileNotFoundException();
            case JET_err.RequiredLogFilesMissing:
                return new EsentRequiredLogFilesMissingException();
            case JET_err.SoftRecoveryOnBackupDatabase:
                return new EsentSoftRecoveryOnBackupDatabaseException();
            case JET_err.LogFileSizeMismatchDatabasesConsistent:
                return new EsentLogFileSizeMismatchDatabasesConsistentException();
            case JET_err.LogSectorSizeMismatch:
                return new EsentLogSectorSizeMismatchException();
            case JET_err.LogSectorSizeMismatchDatabasesConsistent:
                return new EsentLogSectorSizeMismatchDatabasesConsistentException();
            case JET_err.LogSequenceEndDatabasesConsistent:
                return new EsentLogSequenceEndDatabasesConsistentException();
            case JET_err.StreamingDataNotLogged:
                return new EsentStreamingDataNotLoggedException();
            case JET_err.DatabaseDirtyShutdown:
                return new EsentDatabaseDirtyShutdownException();
            case JET_err.ConsistentTimeMismatch:
                return new EsentConsistentTimeMismatchException();
            case JET_err.DatabasePatchFileMismatch:
                return new EsentDatabasePatchFileMismatchException();
            case JET_err.EndingRestoreLogTooLow:
                return new EsentEndingRestoreLogTooLowException();
            case JET_err.StartingRestoreLogTooHigh:
                return new EsentStartingRestoreLogTooHighException();
            case JET_err.GivenLogFileHasBadSignature:
                return new EsentGivenLogFileHasBadSignatureException();
            case JET_err.GivenLogFileIsNotContiguous:
                return new EsentGivenLogFileIsNotContiguousException();
            case JET_err.MissingRestoreLogFiles:
                return new EsentMissingRestoreLogFilesException();
            case JET_err.MissingFullBackup:
                return new EsentMissingFullBackupException();
            case JET_err.BadBackupDatabaseSize:
                return new EsentBadBackupDatabaseSizeException();
            case JET_err.DatabaseAlreadyUpgraded:
                return new EsentDatabaseAlreadyUpgradedException();
            case JET_err.DatabaseIncompleteUpgrade:
                return new EsentDatabaseIncompleteUpgradeException();
            case JET_err.MissingCurrentLogFiles:
                return new EsentMissingCurrentLogFilesException();
            case JET_err.DbTimeTooOld:
                return new EsentDbTimeTooOldException();
            case JET_err.DbTimeTooNew:
                return new EsentDbTimeTooNewException();
            case JET_err.MissingFileToBackup:
                return new EsentMissingFileToBackupException();
            case JET_err.LogTornWriteDuringHardRestore:
                return new EsentLogTornWriteDuringHardRestoreException();
            case JET_err.LogTornWriteDuringHardRecovery:
                return new EsentLogTornWriteDuringHardRecoveryException();
            case JET_err.LogCorruptDuringHardRestore:
                return new EsentLogCorruptDuringHardRestoreException();
            case JET_err.LogCorruptDuringHardRecovery:
                return new EsentLogCorruptDuringHardRecoveryException();
            case JET_err.MustDisableLoggingForDbUpgrade:
                return new EsentMustDisableLoggingForDbUpgradeException();
            case JET_err.BadRestoreTargetInstance:
                return new EsentBadRestoreTargetInstanceException();
            case JET_err.RecoveredWithoutUndo:
                return new EsentRecoveredWithoutUndoException();
            case JET_err.DatabasesNotFromSameSnapshot:
                return new EsentDatabasesNotFromSameSnapshotException();
            case JET_err.SoftRecoveryOnSnapshot:
                return new EsentSoftRecoveryOnSnapshotException();
            case JET_err.CommittedLogFilesMissing:
                return new EsentCommittedLogFilesMissingException();
            case JET_err.SectorSizeNotSupported:
                return new EsentSectorSizeNotSupportedException();
            case JET_err.RecoveredWithoutUndoDatabasesConsistent:
                return new EsentRecoveredWithoutUndoDatabasesConsistentException();
            case JET_err.CommittedLogFileCorrupt:
                return new EsentCommittedLogFileCorruptException();
            case JET_err.UnicodeTranslationBufferTooSmall:
                return new EsentUnicodeTranslationBufferTooSmallException();
            case JET_err.UnicodeTranslationFail:
                return new EsentUnicodeTranslationFailException();
            case JET_err.UnicodeNormalizationNotSupported:
                return new EsentUnicodeNormalizationNotSupportedException();
            case JET_err.UnicodeLanguageValidationFailure:
                return new EsentUnicodeLanguageValidationFailureException();
            case JET_err.ExistingLogFileHasBadSignature:
                return new EsentExistingLogFileHasBadSignatureException();
            case JET_err.ExistingLogFileIsNotContiguous:
                return new EsentExistingLogFileIsNotContiguousException();
            case JET_err.LogReadVerifyFailure:
                return new EsentLogReadVerifyFailureException();
            case JET_err.SLVReadVerifyFailure:
                return new EsentSLVReadVerifyFailureException();
            case JET_err.CheckpointDepthTooDeep:
                return new EsentCheckpointDepthTooDeepException();
            case JET_err.RestoreOfNonBackupDatabase:
                return new EsentRestoreOfNonBackupDatabaseException();
            case JET_err.LogFileNotCopied:
                return new EsentLogFileNotCopiedException();
            case JET_err.SurrogateBackupInProgress:
                return new EsentSurrogateBackupInProgressException();
            case JET_err.BackupAbortByServer:
                return new EsentBackupAbortByServerException();
            case JET_err.InvalidGrbit:
                return new EsentInvalidGrbitException();
            case JET_err.TermInProgress:
                return new EsentTermInProgressException();
            case JET_err.FeatureNotAvailable:
                return new EsentFeatureNotAvailableException();
            case JET_err.InvalidName:
                return new EsentInvalidNameException();
            case JET_err.InvalidParameter:
                return new EsentInvalidParameterException();
            case JET_err.DatabaseFileReadOnly:
                return new EsentDatabaseFileReadOnlyException();
            case JET_err.InvalidDatabaseId:
                return new EsentInvalidDatabaseIdException();
            case JET_err.OutOfMemory:
                return new EsentOutOfMemoryException();
            case JET_err.OutOfDatabaseSpace:
                return new EsentOutOfDatabaseSpaceException();
            case JET_err.OutOfCursors:
                return new EsentOutOfCursorsException();
            case JET_err.OutOfBuffers:
                return new EsentOutOfBuffersException();
            case JET_err.TooManyIndexes:
                return new EsentTooManyIndexesException();
            case JET_err.TooManyKeys:
                return new EsentTooManyKeysException();
            case JET_err.RecordDeleted:
                return new EsentRecordDeletedException();
            case JET_err.ReadVerifyFailure:
                return new EsentReadVerifyFailureException();
            case JET_err.PageNotInitialized:
                return new EsentPageNotInitializedException();
            case JET_err.OutOfFileHandles:
                return new EsentOutOfFileHandlesException();
            case JET_err.DiskReadVerificationFailure:
                return new EsentDiskReadVerificationFailureException();
            case JET_err.DiskIO:
                return new EsentDiskIOException();
            case JET_err.InvalidPath:
                return new EsentInvalidPathException();
            case JET_err.InvalidSystemPath:
                return new EsentInvalidSystemPathException();
            case JET_err.InvalidLogDirectory:
                return new EsentInvalidLogDirectoryException();
            case JET_err.RecordTooBig:
                return new EsentRecordTooBigException();
            case JET_err.TooManyOpenDatabases:
                return new EsentTooManyOpenDatabasesException();
            case JET_err.InvalidDatabase:
                return new EsentInvalidDatabaseException();
            case JET_err.NotInitialized:
                return new EsentNotInitializedException();
            case JET_err.AlreadyInitialized:
                return new EsentAlreadyInitializedException();
            case JET_err.InitInProgress:
                return new EsentInitInProgressException();
            case JET_err.FileAccessDenied:
                return new EsentFileAccessDeniedException();
            case JET_err.QueryNotSupported:
                return new EsentQueryNotSupportedException();
            case JET_err.SQLLinkNotSupported:
                return new EsentSQLLinkNotSupportedException();
            case JET_err.BufferTooSmall:
                return new EsentBufferTooSmallException();
            case JET_err.TooManyColumns:
                return new EsentTooManyColumnsException();
            case JET_err.ContainerNotEmpty:
                return new EsentContainerNotEmptyException();
            case JET_err.InvalidFilename:
                return new EsentInvalidFilenameException();
            case JET_err.InvalidBookmark:
                return new EsentInvalidBookmarkException();
            case JET_err.ColumnInUse:
                return new EsentColumnInUseException();
            case JET_err.InvalidBufferSize:
                return new EsentInvalidBufferSizeException();
            case JET_err.ColumnNotUpdatable:
                return new EsentColumnNotUpdatableException();
            case JET_err.IndexInUse:
                return new EsentIndexInUseException();
            case JET_err.LinkNotSupported:
                return new EsentLinkNotSupportedException();
            case JET_err.NullKeyDisallowed:
                return new EsentNullKeyDisallowedException();
            case JET_err.NotInTransaction:
                return new EsentNotInTransactionException();
            case JET_err.MustRollback:
                return new EsentMustRollbackException();
            case JET_err.TooManyActiveUsers:
                return new EsentTooManyActiveUsersException();
            case JET_err.InvalidCountry:
                return new EsentInvalidCountryException();
            case JET_err.InvalidLanguageId:
                return new EsentInvalidLanguageIdException();
            case JET_err.InvalidCodePage:
                return new EsentInvalidCodePageException();
            case JET_err.InvalidLCMapStringFlags:
                return new EsentInvalidLCMapStringFlagsException();
            case JET_err.VersionStoreEntryTooBig:
                return new EsentVersionStoreEntryTooBigException();
            case JET_err.VersionStoreOutOfMemoryAndCleanupTimedOut:
                return new EsentVersionStoreOutOfMemoryAndCleanupTimedOutException();
            case JET_err.VersionStoreOutOfMemory:
                return new EsentVersionStoreOutOfMemoryException();
            case JET_err.CurrencyStackOutOfMemory:
                return new EsentCurrencyStackOutOfMemoryException();
            case JET_err.CannotIndex:
                return new EsentCannotIndexException();
            case JET_err.RecordNotDeleted:
                return new EsentRecordNotDeletedException();
            case JET_err.TooManyMempoolEntries:
                return new EsentTooManyMempoolEntriesException();
            case JET_err.OutOfObjectIDs:
                return new EsentOutOfObjectIDsException();
            case JET_err.OutOfLongValueIDs:
                return new EsentOutOfLongValueIDsException();
            case JET_err.OutOfAutoincrementValues:
                return new EsentOutOfAutoincrementValuesException();
            case JET_err.OutOfDbtimeValues:
                return new EsentOutOfDbtimeValuesException();
            case JET_err.OutOfSequentialIndexValues:
                return new EsentOutOfSequentialIndexValuesException();
            case JET_err.RunningInOneInstanceMode:
                return new EsentRunningInOneInstanceModeException();
            case JET_err.RunningInMultiInstanceMode:
                return new EsentRunningInMultiInstanceModeException();
            case JET_err.SystemParamsAlreadySet:
                return new EsentSystemParamsAlreadySetException();
            case JET_err.SystemPathInUse:
                return new EsentSystemPathInUseException();
            case JET_err.LogFilePathInUse:
                return new EsentLogFilePathInUseException();
            case JET_err.TempPathInUse:
                return new EsentTempPathInUseException();
            case JET_err.InstanceNameInUse:
                return new EsentInstanceNameInUseException();
            case JET_err.InstanceUnavailable:
                return new EsentInstanceUnavailableException();
            case JET_err.DatabaseUnavailable:
                return new EsentDatabaseUnavailableException();
            case JET_err.InstanceUnavailableDueToFatalLogDiskFull:
                return new EsentInstanceUnavailableDueToFatalLogDiskFullException();
            case JET_err.OutOfSessions:
                return new EsentOutOfSessionsException();
            case JET_err.WriteConflict:
                return new EsentWriteConflictException();
            case JET_err.TransTooDeep:
                return new EsentTransTooDeepException();
            case JET_err.InvalidSesid:
                return new EsentInvalidSesidException();
            case JET_err.WriteConflictPrimaryIndex:
                return new EsentWriteConflictPrimaryIndexException();
            case JET_err.InTransaction:
                return new EsentInTransactionException();
            case JET_err.RollbackRequired:
                return new EsentRollbackRequiredException();
            case JET_err.TransReadOnly:
                return new EsentTransReadOnlyException();
            case JET_err.SessionWriteConflict:
                return new EsentSessionWriteConflictException();
            case JET_err.RecordTooBigForBackwardCompatibility:
                return new EsentRecordTooBigForBackwardCompatibilityException();
            case JET_err.CannotMaterializeForwardOnlySort:
                return new EsentCannotMaterializeForwardOnlySortException();
            case JET_err.SesidTableIdMismatch:
                return new EsentSesidTableIdMismatchException();
            case JET_err.InvalidInstance:
                return new EsentInvalidInstanceException();
            case JET_err.DirtyShutdown:
                return new EsentDirtyShutdownException();
            case JET_err.ReadPgnoVerifyFailure:
                return new EsentReadPgnoVerifyFailureException();
            case JET_err.ReadLostFlushVerifyFailure:
                return new EsentReadLostFlushVerifyFailureException();
            case JET_err.MustCommitDistributedTransactionToLevel0:
                return new EsentMustCommitDistributedTransactionToLevel0Exception();
            case JET_err.DistributedTransactionAlreadyPreparedToCommit:
                return new EsentDistributedTransactionAlreadyPreparedToCommitException();
            case JET_err.NotInDistributedTransaction:
                return new EsentNotInDistributedTransactionException();
            case JET_err.DistributedTransactionNotYetPreparedToCommit:
                return new EsentDistributedTransactionNotYetPreparedToCommitException();
            case JET_err.CannotNestDistributedTransactions:
                return new EsentCannotNestDistributedTransactionsException();
            case JET_err.DTCMissingCallback:
                return new EsentDTCMissingCallbackException();
            case JET_err.DTCMissingCallbackOnRecovery:
                return new EsentDTCMissingCallbackOnRecoveryException();
            case JET_err.DTCCallbackUnexpectedError:
                return new EsentDTCCallbackUnexpectedErrorException();
            case JET_err.DatabaseDuplicate:
                return new EsentDatabaseDuplicateException();
            case JET_err.DatabaseInUse:
                return new EsentDatabaseInUseException();
            case JET_err.DatabaseNotFound:
                return new EsentDatabaseNotFoundException();
            case JET_err.DatabaseInvalidName:
                return new EsentDatabaseInvalidNameException();
            case JET_err.DatabaseInvalidPages:
                return new EsentDatabaseInvalidPagesException();
            case JET_err.DatabaseCorrupted:
                return new EsentDatabaseCorruptedException();
            case JET_err.DatabaseLocked:
                return new EsentDatabaseLockedException();
            case JET_err.CannotDisableVersioning:
                return new EsentCannotDisableVersioningException();
            case JET_err.InvalidDatabaseVersion:
                return new EsentInvalidDatabaseVersionException();
            case JET_err.Database200Format:
                return new EsentDatabase200FormatException();
            case JET_err.Database400Format:
                return new EsentDatabase400FormatException();
            case JET_err.Database500Format:
                return new EsentDatabase500FormatException();
            case JET_err.PageSizeMismatch:
                return new EsentPageSizeMismatchException();
            case JET_err.TooManyInstances:
                return new EsentTooManyInstancesException();
            case JET_err.DatabaseSharingViolation:
                return new EsentDatabaseSharingViolationException();
            case JET_err.AttachedDatabaseMismatch:
                return new EsentAttachedDatabaseMismatchException();
            case JET_err.DatabaseInvalidPath:
                return new EsentDatabaseInvalidPathException();
            case JET_err.DatabaseIdInUse:
                return new EsentDatabaseIdInUseException();
            case JET_err.ForceDetachNotAllowed:
                return new EsentForceDetachNotAllowedException();
            case JET_err.CatalogCorrupted:
                return new EsentCatalogCorruptedException();
            case JET_err.PartiallyAttachedDB:
                return new EsentPartiallyAttachedDBException();
            case JET_err.DatabaseSignInUse:
                return new EsentDatabaseSignInUseException();
            case JET_err.DatabaseCorruptedNoRepair:
                return new EsentDatabaseCorruptedNoRepairException();
            case JET_err.InvalidCreateDbVersion:
                return new EsentInvalidCreateDbVersionException();
            case JET_err.DatabaseIncompleteIncrementalReseed:
                return new EsentDatabaseIncompleteIncrementalReseedException();
            case JET_err.DatabaseInvalidIncrementalReseed:
                return new EsentDatabaseInvalidIncrementalReseedException();
            case JET_err.DatabaseFailedIncrementalReseed:
                return new EsentDatabaseFailedIncrementalReseedException();
            case JET_err.NoAttachmentsFailedIncrementalReseed:
                return new EsentNoAttachmentsFailedIncrementalReseedException();
            case JET_err.TableLocked:
                return new EsentTableLockedException();
            case JET_err.TableDuplicate:
                return new EsentTableDuplicateException();
            case JET_err.TableInUse:
                return new EsentTableInUseException();
            case JET_err.ObjectNotFound:
                return new EsentObjectNotFoundException();
            case JET_err.DensityInvalid:
                return new EsentDensityInvalidException();
            case JET_err.TableNotEmpty:
                return new EsentTableNotEmptyException();
            case JET_err.InvalidTableId:
                return new EsentInvalidTableIdException();
            case JET_err.TooManyOpenTables:
                return new EsentTooManyOpenTablesException();
            case JET_err.IllegalOperation:
                return new EsentIllegalOperationException();
            case JET_err.TooManyOpenTablesAndCleanupTimedOut:
                return new EsentTooManyOpenTablesAndCleanupTimedOutException();
            case JET_err.ObjectDuplicate:
                return new EsentObjectDuplicateException();
            case JET_err.InvalidObject:
                return new EsentInvalidObjectException();
            case JET_err.CannotDeleteTempTable:
                return new EsentCannotDeleteTempTableException();
            case JET_err.CannotDeleteSystemTable:
                return new EsentCannotDeleteSystemTableException();
            case JET_err.CannotDeleteTemplateTable:
                return new EsentCannotDeleteTemplateTableException();
            case JET_err.ExclusiveTableLockRequired:
                return new EsentExclusiveTableLockRequiredException();
            case JET_err.FixedDDL:
                return new EsentFixedDDLException();
            case JET_err.FixedInheritedDDL:
                return new EsentFixedInheritedDDLException();
            case JET_err.CannotNestDDL:
                return new EsentCannotNestDDLException();
            case JET_err.DDLNotInheritable:
                return new EsentDDLNotInheritableException();
            case JET_err.InvalidSettings:
                return new EsentInvalidSettingsException();
            case JET_err.ClientRequestToStopJetService:
                return new EsentClientRequestToStopJetServiceException();
            case JET_err.CannotAddFixedVarColumnToDerivedTable:
                return new EsentCannotAddFixedVarColumnToDerivedTableException();
            case JET_err.IndexCantBuild:
                return new EsentIndexCantBuildException();
            case JET_err.IndexHasPrimary:
                return new EsentIndexHasPrimaryException();
            case JET_err.IndexDuplicate:
                return new EsentIndexDuplicateException();
            case JET_err.IndexNotFound:
                return new EsentIndexNotFoundException();
            case JET_err.IndexMustStay:
                return new EsentIndexMustStayException();
            case JET_err.IndexInvalidDef:
                return new EsentIndexInvalidDefException();
            case JET_err.InvalidCreateIndex:
                return new EsentInvalidCreateIndexException();
            case JET_err.TooManyOpenIndexes:
                return new EsentTooManyOpenIndexesException();
            case JET_err.MultiValuedIndexViolation:
                return new EsentMultiValuedIndexViolationException();
            case JET_err.IndexBuildCorrupted:
                return new EsentIndexBuildCorruptedException();
            case JET_err.PrimaryIndexCorrupted:
                return new EsentPrimaryIndexCorruptedException();
            case JET_err.SecondaryIndexCorrupted:
                return new EsentSecondaryIndexCorruptedException();
            case JET_err.InvalidIndexId:
                return new EsentInvalidIndexIdException();
            case JET_err.IndexTuplesSecondaryIndexOnly:
                return new EsentIndexTuplesSecondaryIndexOnlyException();
            case JET_err.IndexTuplesTooManyColumns:
                return new EsentIndexTuplesTooManyColumnsException();
            case JET_err.IndexTuplesNonUniqueOnly:
                return new EsentIndexTuplesNonUniqueOnlyException();
            case JET_err.IndexTuplesTextBinaryColumnsOnly:
                return new EsentIndexTuplesTextBinaryColumnsOnlyException();
            case JET_err.IndexTuplesVarSegMacNotAllowed:
                return new EsentIndexTuplesVarSegMacNotAllowedException();
            case JET_err.IndexTuplesInvalidLimits:
                return new EsentIndexTuplesInvalidLimitsException();
            case JET_err.IndexTuplesCannotRetrieveFromIndex:
                return new EsentIndexTuplesCannotRetrieveFromIndexException();
            case JET_err.IndexTuplesKeyTooSmall:
                return new EsentIndexTuplesKeyTooSmallException();
            case JET_err.ColumnLong:
                return new EsentColumnLongException();
            case JET_err.ColumnNoChunk:
                return new EsentColumnNoChunkException();
            case JET_err.ColumnDoesNotFit:
                return new EsentColumnDoesNotFitException();
            case JET_err.NullInvalid:
                return new EsentNullInvalidException();
            case JET_err.ColumnIndexed:
                return new EsentColumnIndexedException();
            case JET_err.ColumnTooBig:
                return new EsentColumnTooBigException();
            case JET_err.ColumnNotFound:
                return new EsentColumnNotFoundException();
            case JET_err.ColumnDuplicate:
                return new EsentColumnDuplicateException();
            case JET_err.MultiValuedColumnMustBeTagged:
                return new EsentMultiValuedColumnMustBeTaggedException();
            case JET_err.ColumnRedundant:
                return new EsentColumnRedundantException();
            case JET_err.InvalidColumnType:
                return new EsentInvalidColumnTypeException();
            case JET_err.TaggedNotNULL:
                return new EsentTaggedNotNULLException();
            case JET_err.NoCurrentIndex:
                return new EsentNoCurrentIndexException();
            case JET_err.KeyIsMade:
                return new EsentKeyIsMadeException();
            case JET_err.BadColumnId:
                return new EsentBadColumnIdException();
            case JET_err.BadItagSequence:
                return new EsentBadItagSequenceException();
            case JET_err.ColumnInRelationship:
                return new EsentColumnInRelationshipException();
            case JET_err.CannotBeTagged:
                return new EsentCannotBeTaggedException();
            case JET_err.DefaultValueTooBig:
                return new EsentDefaultValueTooBigException();
            case JET_err.MultiValuedDuplicate:
                return new EsentMultiValuedDuplicateException();
            case JET_err.LVCorrupted:
                return new EsentLVCorruptedException();
            case JET_err.MultiValuedDuplicateAfterTruncation:
                return new EsentMultiValuedDuplicateAfterTruncationException();
            case JET_err.DerivedColumnCorruption:
                return new EsentDerivedColumnCorruptionException();
            case JET_err.InvalidPlaceholderColumn:
                return new EsentInvalidPlaceholderColumnException();
            case JET_err.ColumnCannotBeCompressed:
                return new EsentColumnCannotBeCompressedException();
            case JET_err.RecordNotFound:
                return new EsentRecordNotFoundException();
            case JET_err.RecordNoCopy:
                return new EsentRecordNoCopyException();
            case JET_err.NoCurrentRecord:
                return new EsentNoCurrentRecordException();
            case JET_err.RecordPrimaryChanged:
                return new EsentRecordPrimaryChangedException();
            case JET_err.KeyDuplicate:
                return new EsentKeyDuplicateException();
            case JET_err.AlreadyPrepared:
                return new EsentAlreadyPreparedException();
            case JET_err.KeyNotMade:
                return new EsentKeyNotMadeException();
            case JET_err.UpdateNotPrepared:
                return new EsentUpdateNotPreparedException();
            case JET_err.DataHasChanged:
                return new EsentDataHasChangedException();
            case JET_err.LanguageNotSupported:
                return new EsentLanguageNotSupportedException();
            case JET_err.DecompressionFailed:
                return new EsentDecompressionFailedException();
            case JET_err.UpdateMustVersion:
                return new EsentUpdateMustVersionException();
            case JET_err.TooManySorts:
                return new EsentTooManySortsException();
            case JET_err.InvalidOnSort:
                return new EsentInvalidOnSortException();
            case JET_err.TempFileOpenError:
                return new EsentTempFileOpenErrorException();
            case JET_err.TooManyAttachedDatabases:
                return new EsentTooManyAttachedDatabasesException();
            case JET_err.DiskFull:
                return new EsentDiskFullException();
            case JET_err.PermissionDenied:
                return new EsentPermissionDeniedException();
            case JET_err.FileNotFound:
                return new EsentFileNotFoundException();
            case JET_err.FileInvalidType:
                return new EsentFileInvalidTypeException();
            case JET_err.AfterInitialization:
                return new EsentAfterInitializationException();
            case JET_err.LogCorrupted:
                return new EsentLogCorruptedException();
            case JET_err.InvalidOperation:
                return new EsentInvalidOperationException();
            case JET_err.AccessDenied:
                return new EsentAccessDeniedException();
            case JET_err.TooManySplits:
                return new EsentTooManySplitsException();
            case JET_err.SessionSharingViolation:
                return new EsentSessionSharingViolationException();
            case JET_err.EntryPointNotFound:
                return new EsentEntryPointNotFoundException();
            case JET_err.SessionContextAlreadySet:
                return new EsentSessionContextAlreadySetException();
            case JET_err.SessionContextNotSetByThisThread:
                return new EsentSessionContextNotSetByThisThreadException();
            case JET_err.SessionInUse:
                return new EsentSessionInUseException();
            case JET_err.RecordFormatConversionFailed:
                return new EsentRecordFormatConversionFailedException();
            case JET_err.OneDatabasePerSession:
                return new EsentOneDatabasePerSessionException();
            case JET_err.RollbackError:
                return new EsentRollbackErrorException();
            case JET_err.DatabaseAlreadyRunningMaintenance:
                return new EsentDatabaseAlreadyRunningMaintenanceException();
            case JET_err.CallbackFailed:
                return new EsentCallbackFailedException();
            case JET_err.CallbackNotResolved:
                return new EsentCallbackNotResolvedException();
            case JET_err.SpaceHintsInvalid:
                return new EsentSpaceHintsInvalidException();
            case JET_err.SLVSpaceCorrupted:
                return new EsentSLVSpaceCorruptedException();
            case JET_err.SLVCorrupted:
                return new EsentSLVCorruptedException();
            case JET_err.SLVColumnDefaultValueNotAllowed:
                return new EsentSLVColumnDefaultValueNotAllowedException();
            case JET_err.SLVStreamingFileMissing:
                return new EsentSLVStreamingFileMissingException();
            case JET_err.SLVDatabaseMissing:
                return new EsentSLVDatabaseMissingException();
            case JET_err.SLVStreamingFileAlreadyExists:
                return new EsentSLVStreamingFileAlreadyExistsException();
            case JET_err.SLVInvalidPath:
                return new EsentSLVInvalidPathException();
            case JET_err.SLVStreamingFileNotCreated:
                return new EsentSLVStreamingFileNotCreatedException();
            case JET_err.SLVStreamingFileReadOnly:
                return new EsentSLVStreamingFileReadOnlyException();
            case JET_err.SLVHeaderBadChecksum:
                return new EsentSLVHeaderBadChecksumException();
            case JET_err.SLVHeaderCorrupted:
                return new EsentSLVHeaderCorruptedException();
            case JET_err.SLVPagesNotFree:
                return new EsentSLVPagesNotFreeException();
            case JET_err.SLVPagesNotReserved:
                return new EsentSLVPagesNotReservedException();
            case JET_err.SLVPagesNotCommitted:
                return new EsentSLVPagesNotCommittedException();
            case JET_err.SLVPagesNotDeleted:
                return new EsentSLVPagesNotDeletedException();
            case JET_err.SLVSpaceWriteConflict:
                return new EsentSLVSpaceWriteConflictException();
            case JET_err.SLVRootStillOpen:
                return new EsentSLVRootStillOpenException();
            case JET_err.SLVProviderNotLoaded:
                return new EsentSLVProviderNotLoadedException();
            case JET_err.SLVEAListCorrupt:
                return new EsentSLVEAListCorruptException();
            case JET_err.SLVRootNotSpecified:
                return new EsentSLVRootNotSpecifiedException();
            case JET_err.SLVRootPathInvalid:
                return new EsentSLVRootPathInvalidException();
            case JET_err.SLVEAListZeroAllocation:
                return new EsentSLVEAListZeroAllocationException();
            case JET_err.SLVColumnCannotDelete:
                return new EsentSLVColumnCannotDeleteException();
            case JET_err.SLVOwnerMapAlreadyExists:
                return new EsentSLVOwnerMapAlreadyExistsException();
            case JET_err.SLVOwnerMapCorrupted:
                return new EsentSLVOwnerMapCorruptedException();
            case JET_err.SLVOwnerMapPageNotFound:
                return new EsentSLVOwnerMapPageNotFoundException();
            case JET_err.SLVFileStale:
                return new EsentSLVFileStaleException();
            case JET_err.SLVFileInUse:
                return new EsentSLVFileInUseException();
            case JET_err.SLVStreamingFileInUse:
                return new EsentSLVStreamingFileInUseException();
            case JET_err.SLVFileIO:
                return new EsentSLVFileIOException();
            case JET_err.SLVStreamingFileFull:
                return new EsentSLVStreamingFileFullException();
            case JET_err.SLVFileInvalidPath:
                return new EsentSLVFileInvalidPathException();
            case JET_err.SLVFileAccessDenied:
                return new EsentSLVFileAccessDeniedException();
            case JET_err.SLVFileNotFound:
                return new EsentSLVFileNotFoundException();
            case JET_err.SLVFileUnknown:
                return new EsentSLVFileUnknownException();
            case JET_err.SLVEAListTooBig:
                return new EsentSLVEAListTooBigException();
            case JET_err.SLVProviderVersionMismatch:
                return new EsentSLVProviderVersionMismatchException();
            case JET_err.SLVBufferTooSmall:
                return new EsentSLVBufferTooSmallException();
            case JET_err.OSSnapshotInvalidSequence:
                return new EsentOSSnapshotInvalidSequenceException();
            case JET_err.OSSnapshotTimeOut:
                return new EsentOSSnapshotTimeOutException();
            case JET_err.OSSnapshotNotAllowed:
                return new EsentOSSnapshotNotAllowedException();
            case JET_err.OSSnapshotInvalidSnapId:
                return new EsentOSSnapshotInvalidSnapIdException();
            case JET_err.TooManyTestInjections:
                return new EsentTooManyTestInjectionsException();
            case JET_err.TestInjectionNotSupported:
                return new EsentTestInjectionNotSupportedException();
            case JET_err.InvalidLogDataSequence:
                return new EsentInvalidLogDataSequenceException();
            case JET_err.LSCallbackNotSpecified:
                return new EsentLSCallbackNotSpecifiedException();
            case JET_err.LSAlreadySet:
                return new EsentLSAlreadySetException();
            case JET_err.LSNotSet:
                return new EsentLSNotSetException();
            case JET_err.FileIOSparse:
                return new EsentFileIOSparseException();
            case JET_err.FileIOBeyondEOF:
                return new EsentFileIOBeyondEOFException();
            case JET_err.FileIOAbort:
                return new EsentFileIOAbortException();
            case JET_err.FileIORetry:
                return new EsentFileIORetryException();
            case JET_err.FileIOFail:
                return new EsentFileIOFailException();
            case JET_err.FileCompressed:
                return new EsentFileCompressedException();
            default:
                // This could be a new error introduced in a newer version of Esent. Try to look up the description.
                IntPtr errNum = new IntPtr((int)err);
                string description;
                int wrn = Api.Impl.JetGetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.ErrorToString, ref errNum, out description, 1024);
                err = (JET_err)errNum.ToInt32();
                if ((int)JET_wrn.Success != wrn)
                {
                    description = "Unknown error";
                }
                
                return new EsentErrorException(description, err);
            }
        }
    }
}
