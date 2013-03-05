// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyColumnConverter.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code to extend the ColumnConverter class with methods to make a key.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Extends the ColumnConverter class with methods to make a key.
    /// </summary>
    /// <typeparam name="TColumn">The type of the column.</typeparam>
    internal sealed class KeyColumnConverter<TColumn> : ColumnConverter<TColumn>
    {
        /// <summary>
        /// The MakeKey delegate for this object.
        /// </summary>
        private readonly MakeKeyDelegate keyMaker;

        /// <summary>
        /// Initializes a new instance of the KeyColumnConverter class.
        /// </summary>
        public KeyColumnConverter()
        {
            const string MakeKeyMethodName = "MakeKey";
            Type[] arguments = new[] { typeof(JET_SESID), typeof(JET_TABLEID), typeof(TColumn), typeof(MakeKeyGrbit) };

            // Look for a private method in this class that takes the appropriate arguments, 
            // otherwise a method on the Api class.
            MethodInfo method = typeof(KeyColumnConverter<TColumn>).GetMethod(
                                             MakeKeyMethodName,
                                             BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.ExactBinding,
                                             null,
                                             arguments,
                                             null) ?? typeof(Api).GetMethod(
                                                          MakeKeyMethodName,
                                                          BindingFlags.Static | BindingFlags.Public | BindingFlags.ExactBinding,
                                                          null,
                                                          arguments,
                                                          null);
            if (null != method)
            {
                this.keyMaker = (MakeKeyDelegate)Delegate.CreateDelegate(typeof(MakeKeyDelegate), method);
                RuntimeHelpers.PrepareDelegate(this.keyMaker);
            }
            else
            {
                throw new ArgumentOutOfRangeException("type", typeof(TColumn), "Not supported for MakeKey");                
            }
        }

        /// <summary>
        /// Represents a MakeKey operation.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to make the key in.</param>
        /// <param name="value">The value to make the key with.</param>
        /// <param name="grbit">Make key options.</param>
        public delegate void MakeKeyDelegate(JET_SESID sesid, JET_TABLEID tableid, TColumn value, MakeKeyGrbit grbit);

        /// <summary>
        /// Gets a delegate that can be used to call JetMakeKey with an object of
        /// type <see cref="Type"/>.
        /// </summary>
        public MakeKeyDelegate KeyMaker
        {
            get
            {
                return this.keyMaker;
            }
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="Api.JetSeek"/>
        /// and <see cref="Api.JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        private static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, DateTime data, MakeKeyGrbit grbit)
        {
            Api.MakeKey(sesid, tableid, data.Ticks, grbit);
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="Api.JetSeek"/>
        /// and <see cref="Api.JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        private static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, TimeSpan data, MakeKeyGrbit grbit)
        {
            Api.MakeKey(sesid, tableid, data.Ticks, grbit);
        }

        /// <summary>
        /// Constructs a search key that may then be used by <see cref="Api.JetSeek"/>
        /// and <see cref="Api.JetSetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="grbit">Key options.</param>
        private static void MakeKey(JET_SESID sesid, JET_TABLEID tableid, string data, MakeKeyGrbit grbit)
        {
            Api.MakeKey(sesid, tableid, data, Encoding.Unicode, grbit);
        }
    }
}
