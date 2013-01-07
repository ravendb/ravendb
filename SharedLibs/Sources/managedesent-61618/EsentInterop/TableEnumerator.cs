//-----------------------------------------------------------------------
// <copyright file="TableEnumerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Base class for enumerators that walk a table.
    /// </summary>
    /// <typeparam name="T">The type returned by the enumerator.</typeparam>
    internal abstract class TableEnumerator<T> : IEnumerator<T>
    {
        /// <summary>
        /// True if we are at the end of the table.
        /// </summary>
        private bool isAtEnd;

        /// <summary>
        /// True if we need to move to the first record in the table.
        /// </summary>
        private bool moveToFirst = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableEnumerator{T}"/> class.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        protected TableEnumerator(JET_SESID sesid)
        {
            this.Sesid = sesid;
            this.TableidToEnumerate = JET_TABLEID.Nil;
        }

        /// <summary>
        /// Gets the current entry.
        /// </summary>
        public T Current { get; private set; }

        /// <summary>
        /// Gets the current entry.
        /// </summary>
        object IEnumerator.Current
        {
            [DebuggerStepThrough]
            get { return this.Current; }
        }

        /// <summary>
        /// Gets the session used for the enumeration.
        /// </summary>
        protected JET_SESID Sesid { get; private set; }

        /// <summary>
        /// Gets or sets the table being enumerated.
        /// </summary>
        protected JET_TABLEID TableidToEnumerate { get; set; }

        /// <summary>
        /// Resets the enumerator. The next call to MoveNext will move
        /// to the first entry.
        /// </summary>
        public void Reset()
        {
            this.isAtEnd = false;
            this.moveToFirst = true;
        }

        /// <summary>
        /// Disposes of any resources the enumerator is using.
        /// </summary>
        public void Dispose()
        {
            this.CloseTable();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Move to the next entry.
        /// </summary>
        /// <returns>
        /// True if an entry was found, false otherwise.
        /// </returns>
        public bool MoveNext()
        {
            // Moving to the end is sticky (until Reset is called)
            if (this.isAtEnd)
            {
                return false;
            }

            if (JET_TABLEID.Nil == this.TableidToEnumerate)
            {
                Debug.Assert(this.moveToFirst, "Table has not been opened so moveToFirst should be true");
                this.OpenTable();
            }

            bool needMoveNext = true;
            if (this.moveToFirst)
            {
                if (!Api.TryMoveFirst(this.Sesid, this.TableidToEnumerate))
                {
                    this.isAtEnd = true;
                    return false;                    
                }

                this.moveToFirst = false;
                needMoveNext = false;
            }           

            while (needMoveNext || this.SkipCurrent())
            {
                if (!Api.TryMoveNext(this.Sesid, this.TableidToEnumerate))
                {
                    this.isAtEnd = true;
                    return false;
                }

                needMoveNext = false;
            }

            this.Current = this.GetCurrent();
            return true;
        }

        /// <summary>
        /// Open the table to be enumerated. This should set <see cref="TableidToEnumerate"/>.
        /// </summary>
        protected abstract void OpenTable();

        /// <summary>
        /// Gets the entry the cursor is currently positioned on.
        /// </summary>
        /// <returns>The entry the cursor is currently positioned on.</returns>
        protected abstract T GetCurrent();

        /// <summary>
        /// Determine if the current entry in the table being enumerated should
        /// be skipped (not returned). By default this is false.
        /// </summary>
        /// <returns>True if the current entry should be skipped.</returns>
        protected virtual bool SkipCurrent()
        {
            return false;
        }

        /// <summary>
        /// Closes the table being enumerated.
        /// </summary>
        protected virtual void CloseTable()
        {
            if (JET_TABLEID.Nil != this.TableidToEnumerate)
            {
                Api.JetCloseTable(this.Sesid, this.TableidToEnumerate);
            }

            this.TableidToEnumerate = JET_TABLEID.Nil;
        }
    }
}