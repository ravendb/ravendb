//-----------------------------------------------------------------------
// <copyright file="DisposableObjectId.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A unique identifier for a disposable object.
    /// </summary>
    internal struct DisposableObjectId : IComparable<DisposableObjectId>
    {
        /// <summary>
        /// Gets or sets the value of the Id.
        /// </summary>
        public long Value { get; set; }

        #region IComparable<DisposableObjectId> Members

        /// <summary>
        /// Compare two DisposableObjectsIds.
        /// </summary>
        /// <param name="left">The first id.</param>
        /// <param name="right">The second id.</param>
        /// <returns>True if the first id is less than the second id.</returns>
        public static bool operator <(DisposableObjectId left, DisposableObjectId right)
        {
            return left.CompareTo(right) < 0;
        }

        /// <summary>
        /// Compare two DisposableObjectsIds.
        /// </summary>
        /// <param name="left">The first id.</param>
        /// <param name="right">The second id.</param>
        /// <returns>True if the first id is greater than the second id.</returns>
        public static bool operator >(DisposableObjectId left, DisposableObjectId right)
        {
            return left.CompareTo(right) > 0;
        }

        /// <summary>
        /// Compare two DisposableObjectIds. This can be used by structures
        /// that sort objects.
        /// </summary>
        /// <param name="otherId">The id to compare with.</param>
        /// <returns>An indication of their relative values.</returns>
        public int CompareTo(DisposableObjectId otherId)
        {
            return this.Value.CompareTo(otherId.Value);
        }

        #endregion
    }
}