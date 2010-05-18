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
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Extends the ColumnConverter class with methods to make a key.
    /// </summary>
    internal sealed class KeyColumnConverter : ColumnConverter
    {
        /// <summary>
        /// A mapping of types to MakeKey functions.
        /// </summary>
        private static readonly IDictionary<Type, MakeKeyDelegate> MakeKeyDelegates = new Dictionary<Type, MakeKeyDelegate>
        {
            { typeof(bool), (s, t, o, g) => Api.MakeKey(s, t, (bool) o, g) },
            { typeof(byte), (s, t, o, g) => Api.MakeKey(s, t, (byte) o, g) },
            { typeof(short), (s, t, o, g) => Api.MakeKey(s, t, (short) o, g) },
            { typeof(ushort), (s, t, o, g) => Api.MakeKey(s, t, (ushort) o, g) },
            { typeof(int), (s, t, o, g) => Api.MakeKey(s, t, (int) o, g) },
            { typeof(uint), (s, t, o, g) => Api.MakeKey(s, t, (uint) o, g) },
            { typeof(long), (s, t, o, g) => Api.MakeKey(s, t, (long) o, g) },
            { typeof(ulong), (s, t, o, g) => Api.MakeKey(s, t, (ulong) o, g) },
            { typeof(float), (s, t, o, g) => Api.MakeKey(s, t, (float) o, g) },
            { typeof(double), (s, t, o, g) => Api.MakeKey(s, t, (double) o, g) },
            { typeof(DateTime), (s, t, o, g) => Api.MakeKey(s, t, ((DateTime) o).Ticks, g) },
            { typeof(TimeSpan), (s, t, o, g) => Api.MakeKey(s, t, ((TimeSpan) o).Ticks, g) },
            { typeof(Guid), (s, t, o, g) => Api.MakeKey(s, t, ((Guid) o).ToByteArray(), g) },
            { typeof(string), (s, t, o, g) => Api.MakeKey(s, t, (string) o, Encoding.Unicode, g) },
        };

        /// <summary>
        /// The MakeKey delegate for this object.
        /// </summary>
        private readonly MakeKeyDelegate makeKey;

        /// <summary>
        /// Initializes a new instance of the KeyColumnConverter class.
        /// </summary>
        /// <param name="type">The type to convert to/from.</param>
        public KeyColumnConverter(Type type) : base(type)
        {
            if (!MakeKeyDelegates.ContainsKey(type))
            {
                throw new ArgumentOutOfRangeException("type", type, "Not supported for MakeKey");
            }

            this.makeKey = MakeKeyDelegates[type];
        }

        /// <summary>
        /// Represents a MakeKey operation.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to make the key in.</param>
        /// <param name="value">The value to make the key with.</param>
        /// <param name="grbit">Make key options.</param>
        public delegate void MakeKeyDelegate(JET_SESID sesid, JET_TABLEID tableid, object value, MakeKeyGrbit grbit);

        /// <summary>
        /// Gets a delegate that can be used to call JetMakeKey with an object of
        /// type <see cref="Type"/>.
        /// </summary>
        public MakeKeyDelegate MakeKey
        {
            get
            {
                return this.makeKey;
            }
        }
    }
}
