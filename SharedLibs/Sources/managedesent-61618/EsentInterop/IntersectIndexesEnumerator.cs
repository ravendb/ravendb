//-----------------------------------------------------------------------
// <copyright file="IntersectIndexesEnumerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Enumerator that can intersect indexes and return the intersected bookmarks.
    /// </summary>
    internal sealed class IntersectIndexesEnumerator : TableEnumerator<byte[]>
    {
        /// <summary>
        /// The ranges to intersect.
        /// </summary>
        private readonly JET_INDEXRANGE[] ranges;

        /// <summary>
        /// The recordlist containing the result of the intersection.
        /// </summary>
        private JET_RECORDLIST recordlist;

        /// <summary>
        /// Initializes a new instance of the <see cref="IntersectIndexesEnumerator"/> class.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        /// <param name="ranges">
        /// The ranges to intersect.
        /// </param>
        public IntersectIndexesEnumerator(JET_SESID sesid, JET_INDEXRANGE[] ranges) : base(sesid)
        {
            this.ranges = ranges;
        }

        /// <summary>
        /// Open the table to be enumerated. This should set <see cref="TableEnumerator{T}.TableidToEnumerate"/>.
        /// </summary>
        protected override void OpenTable()
        {
            Api.JetIntersectIndexes(this.Sesid, this.ranges, this.ranges.Length, out this.recordlist, IntersectIndexesGrbit.None);
            this.TableidToEnumerate = this.recordlist.tableid;
        }

        /// <summary>
        /// Gets the entry the cursor is currently positioned on.
        /// </summary>
        /// <returns>The entry the cursor is currently positioned on.</returns>
        protected override byte[] GetCurrent()
        {
            return Api.RetrieveColumn(this.Sesid, this.TableidToEnumerate, this.recordlist.columnidBookmark);
        }
    }
}