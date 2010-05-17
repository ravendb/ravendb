// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ColumnConverter.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Contains methods to set and get data from the ESENT database.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Reflection;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Contains methods to set and get data from the ESENT
    /// database.
    /// </summary>
    internal class ColumnConverter
    {
        /// <summary>
        /// A mapping of types to SetColumn functions.
        /// </summary>
        private static readonly IDictionary<Type, SetColumnDelegate> SetColumnDelegates = new Dictionary<Type, SetColumnDelegate>
        {
            { typeof(bool), (s, t, c, o) => Api.SetColumn(s, t, c, (bool) o) },
            { typeof(byte), (s, t, c, o) => Api.SetColumn(s, t, c, (byte) o) },
            { typeof(short), (s, t, c, o) => Api.SetColumn(s, t, c, (short) o) },
            { typeof(ushort), (s, t, c, o) => Api.SetColumn(s, t, c, (ushort) o) },
            { typeof(int), (s, t, c, o) => Api.SetColumn(s, t, c, (int) o) },
            { typeof(uint), (s, t, c, o) => Api.SetColumn(s, t, c, (uint) o) },
            { typeof(long), (s, t, c, o) => Api.SetColumn(s, t, c, (long) o) },
            { typeof(ulong), (s, t, c, o) => Api.SetColumn(s, t, c, (ulong) o) },
            { typeof(float), (s, t, c, o) => Api.SetColumn(s, t, c, (float) o) },
            { typeof(double), (s, t, c, o) => Api.SetColumn(s, t, c, (double) o) },
            { typeof(DateTime), (s, t, c, o) => Api.SetColumn(s, t, c, ((DateTime) o).Ticks) },
            { typeof(TimeSpan), (s, t, c, o) => Api.SetColumn(s, t, c, ((TimeSpan) o).Ticks) },
            { typeof(Guid), (s, t, c, o) => Api.SetColumn(s, t, c, (Guid) o) },
            { typeof(string), (s, t, c, o) => Api.SetColumn(s, t, c, (string) o, Encoding.Unicode) },
        };

        /// <summary>
        /// A mapping of types to RetrieveColumn functions.
        /// </summary>
        private static readonly IDictionary<Type, RetrieveColumnDelegate> RetrieveColumnDelegates = new Dictionary<Type, RetrieveColumnDelegate>
        {
            { typeof(bool), (s, t, c) => Api.RetrieveColumnAsBoolean(s, t, c) },
            { typeof(byte), (s, t, c) => Api.RetrieveColumnAsByte(s, t, c) },
            { typeof(short), (s, t, c) => Api.RetrieveColumnAsInt16(s, t, c) },
            { typeof(ushort), (s, t, c) => Api.RetrieveColumnAsUInt16(s, t, c) },
            { typeof(int), (s, t, c) => Api.RetrieveColumnAsInt32(s, t, c) },
            { typeof(uint), (s, t, c) => Api.RetrieveColumnAsUInt32(s, t, c) },
            { typeof(long), (s, t, c) => Api.RetrieveColumnAsInt64(s, t, c) },
            { typeof(ulong), (s, t, c) => Api.RetrieveColumnAsUInt64(s, t, c) },
            { typeof(float), (s, t, c) => Api.RetrieveColumnAsFloat(s, t, c) },
            { typeof(double), (s, t, c) => Api.RetrieveColumnAsDouble(s, t, c) },
            { typeof(Guid), (s, t, c) => Api.RetrieveColumnAsGuid(s, t, c) },
            { typeof(string), (s, t, c) => Api.RetrieveColumnAsString(s, t, c) },
            { typeof(DateTime), (s, t, c) => RetrieveDateTime(s, t, c) },
            { typeof(TimeSpan), (s, t, c) => RetrieveTimeSpan(s, t, c) },
        };

        /// <summary>
        /// A mapping of types to ESENT column types.
        /// </summary>
        private static readonly IDictionary<Type, JET_coltyp> Coltyps = new Dictionary<Type, JET_coltyp>
        {
            { typeof(bool), JET_coltyp.Bit },
            { typeof(byte), JET_coltyp.UnsignedByte },
            { typeof(short), JET_coltyp.Short },
            { typeof(ushort), JET_coltyp.Binary },
            { typeof(int), JET_coltyp.Long },
            { typeof(uint), JET_coltyp.Binary },
            { typeof(long), JET_coltyp.Currency },
            { typeof(ulong), JET_coltyp.Binary },
            { typeof(float), JET_coltyp.IEEESingle },
            { typeof(double), JET_coltyp.IEEEDouble },
            { typeof(DateTime), JET_coltyp.Currency },
            { typeof(TimeSpan), JET_coltyp.Currency },
            { typeof(Guid), JET_coltyp.Binary },
            { typeof(string), JET_coltyp.LongText },
        };

        /// <summary>
        /// The SetColumn delegate for this object.
        /// </summary>
        private readonly SetColumnDelegate setColumn;

        /// <summary>
        /// The RetrieveColumn delegate for this object.
        /// </summary>
        private readonly RetrieveColumnDelegate retrieveColumn;

        /// <summary>
        /// The column type for this object.
        /// </summary>
        private readonly JET_coltyp coltyp;

        /// <summary>
        /// Initializes static members of the ColumnConverter class. This sets up
        /// the conversion ditionaries.
        /// </summary>
        static ColumnConverter()
        {
            AddNullableDelegates<bool>();
            AddNullableDelegates<byte>();
            AddNullableDelegates<short>();
            AddNullableDelegates<ushort>();
            AddNullableDelegates<int>();
            AddNullableDelegates<uint>();
            AddNullableDelegates<long>();
            AddNullableDelegates<ulong>();
            AddNullableDelegates<float>();
            AddNullableDelegates<double>();
            AddNullableDelegates<DateTime>();
            AddNullableDelegates<TimeSpan>();
            AddNullableDelegates<Guid>();
        }

        /// <summary>
        /// Initializes a new instance of the ColumnConverter class.
        /// </summary>
        /// <param name="type">The type to convert to/from.</param>
        public ColumnConverter(Type type)
        {
            if (null == type)
            {
                throw new ArgumentNullException("type");    
            }

            if (!SetColumnDelegates.ContainsKey(type))
            {
                if (!IsSerializable(type))
                {
                    throw new ArgumentOutOfRangeException("type", type, "Not supported for SetColumn");                    
                }

                this.setColumn = (s, t, c, o) => Api.SerializeObjectToColumn(s, t, c, o);
            }
            else
            {
                this.setColumn = SetColumnDelegates[type];
            }

            if (!RetrieveColumnDelegates.ContainsKey(type))
            {
                if (!IsSerializable(type))
                {
                    throw new ArgumentOutOfRangeException("type", type, "Not supported for RetrieveColumn");
                }

                this.retrieveColumn = (s, t, c) => Api.DeserializeObjectFromColumn(s, t, c);
            }
            else
            {
                this.retrieveColumn = RetrieveColumnDelegates[type];
            }

            if (!Coltyps.ContainsKey(type))
            {
                if (!IsSerializable(type))
                {
                    throw new ArgumentOutOfRangeException("type", type, "Has no matching ESENT column type");
                }

                this.coltyp = JET_coltyp.LongBinary;
            }
            else
            {
                this.coltyp = Coltyps[type];
            }
        }

        /// <summary>
        /// Represents a SetColumn operation.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the value in. An update should be prepared.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        public delegate void SetColumnDelegate(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, object value);

        /// <summary>
        /// Represents a RetrieveColumn operation.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the value from.</param>
        /// <param name="columnid">The column to retrieve.</param>
        /// <returns>The retrieved value.</returns>
        public delegate object RetrieveColumnDelegate(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid);

        /// <summary>
        /// Gets the type of database column the value should be stored in.
        /// </summary>
        public JET_coltyp Coltyp
        {
            get
            {
                return this.coltyp;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to set the Key column with an object of
        /// type <see cref="Type"/>.
        /// </summary>
        public SetColumnDelegate SetColumn
        {
            get
            {
                return this.setColumn;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Key column, returning
        /// type <see cref="Type"/>.
        /// </summary>
        public RetrieveColumnDelegate RetrieveColumn
        {
            get
            {
                return this.retrieveColumn;
            }
        }

        /// <summary>
        /// Determine if the given type is a serializable structure.
        /// </summary>
        /// <param name="type">The type to check.</param>
        /// <returns>
        /// True if the type (and the types it contains) are all serializable structures.
        /// </returns>
        private static bool IsSerializable(Type type)
        {
            // Strings are fine (they are immutable)
            if (typeof(string) == type)
            {
                return true;
            }

            // Immutable serializable classes from .NET framework.
            if (typeof(Uri) == type
                || typeof(IPAddress) == type)
            {
                return true;
            }

            // A primitive serializable type is fine
            if (type.IsPrimitive && type.IsSerializable)
            {
                return true;
            }

            // If this isn't a serializable struct, the type definitely isn't serializable
            if (!(type.IsValueType && type.IsSerializable))
            {
                return false;
            }

            // This is a serializable struct. Recursively check that all members are serializable.
            // Unlike classes, structs cannot have cycles in their definitions so a simple enumeration
            // will work.
            foreach (var member in type.GetMembers(BindingFlags.NonPublic | BindingFlags.Instance))
            {
                if (member.MemberType == MemberTypes.Field)
                {
                    var fieldinfo = (FieldInfo) member;
                    if (!IsSerializable(fieldinfo.FieldType))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Generate nullable MakeKey/SetKey delegates for the type.
        /// </summary>
        /// <typeparam name="T">The (non-nullable) type to add delegates for.</typeparam>
        private static void AddNullableDelegates<T>() where T : struct
        {
            AddNullableSetColumn<T>();

            // Retrieve column already returns a nullable object.
            RetrieveColumnDelegates[typeof(T?)] = RetrieveColumnDelegates[typeof(T)];

            // All ESENT columns are nullable
            Coltyps[typeof(T?)] = Coltyps[typeof(T)];
        }

        /// <summary>
        /// Adds a SetColumn delegate that takes a nullable version of the specified type
        /// to the <see cref="SetColumnDelegates"/> object.
        /// </summary>
        /// <typeparam name="T">The type to add the delegate for.</typeparam>
        private static void AddNullableSetColumn<T>() where T : struct
        {
            SetColumnDelegates[typeof(T?)] = MakeNullableSetColumn<T>(SetColumnDelegates[typeof(T)]);
        }

        /// <summary>
        /// Creates a delegate which takes a nullable object and wraps the
        /// non-nullable SetColumn method.
        /// </summary>
        /// <typeparam name="T">The type that will be nullable.</typeparam>
        /// <param name="wrappedSetColumn">The (non-nullable) delegrate to wrap.</param>
        /// <returns>
        /// A SetColumnDelegate that takes a Nullable<typeparamref name="T"/> and
        /// either sets the column to null or calls the wrapped delegate.
        /// </returns>
        private static SetColumnDelegate MakeNullableSetColumn<T>(SetColumnDelegate wrappedSetColumn) where T : struct
        {
            return (s, t, c, o) =>
            {
                if (((T?)o).HasValue)
                {
                    wrappedSetColumn(s, t, c, o);
                }
                else
                {
                    Api.SetColumn(s, t, c, null);
                }
            };
        }

        /// <summary>
        /// Retrieve a nullable date time. We do not use Api.RetrieveColumnAsDateTime because
        /// that stores the value in OADate format, which is less accurate than System.DateTime.
        /// Instead we store a DateTime as its Tick value in an Int64 column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve the value from.</param>
        /// <param name="columnid">The column containing the value.</param>
        /// <returns>A nullable DateTime constructed from the column.</returns>
        private static DateTime? RetrieveDateTime(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            long? ticks = Api.RetrieveColumnAsInt64(sesid, tableid, columnid);
            if (ticks.HasValue)
            {
                return new DateTime(ticks.Value);
            }

            return null;
        }

        /// <summary>
        /// Retrieve a nullable TimeSpan.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve the value from.</param>
        /// <param name="columnid">The column containing the value.</param>
        /// <returns>A nullable TimeSpan constructed from the column.</returns>
        private static TimeSpan? RetrieveTimeSpan(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            long? ticks = Api.RetrieveColumnAsInt64(sesid, tableid, columnid);
            if (ticks.HasValue)
            {
                return new TimeSpan(ticks.Value);
            }

            return null;
        }
    }
}
