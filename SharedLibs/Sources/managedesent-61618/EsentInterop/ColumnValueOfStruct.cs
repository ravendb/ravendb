//-----------------------------------------------------------------------
// <copyright file="ColumnValueOfStruct.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Set a column of a struct type (e.g. Int32/Guid).
    /// </summary>
    /// <typeparam name="T">Type to set.</typeparam>
    public abstract class ColumnValueOfStruct<T> : ColumnValue where T : struct, IEquatable<T>
    {
        /// <summary>
        /// Gets the last set or retrieved value of the column. The
        /// value is returned as a generic object.
        /// </summary>
        public override object ValueAsObject
        {
            get
            {
                return BoxedValueCache<T>.GetBoxedValue(this.Value);
            }
        }

        /// <summary>
        /// Gets or sets the value in the struct.
        /// </summary>
        public T? Value { get; set; }

        /// <summary>
        /// Gets a string representation of this object.
        /// </summary>
        /// <returns>A string representation of this object.</returns>
        public override string ToString()
        {
            return this.Value.ToString();
        }

        /// <summary>
        /// Make sure the retrieved data is exactly the size needed for
        /// the structure. An exception is thrown if there is a mismatch.
        /// </summary>
        /// <param name="count">The size of the retrieved data.</param>
        protected void CheckDataCount(int count)
        {
            if (this.Size != count)
            {
                throw new EsentInvalidColumnException();
            }
        }
    }
}