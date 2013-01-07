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
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Runtime.CompilerServices;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Contains methods to set and get data from the ESENT
    /// database.
    /// </summary>
    /// <typeparam name="TColumn">The type of the column.</typeparam>
    internal class ColumnConverter<TColumn>
    {
        /// <summary>
        /// A mapping of types to RetrieveColumn function names.
        /// </summary>
        private static readonly IDictionary<Type, string> retrieveColumnMethodNames = new Dictionary<Type, string>
        {
            { typeof(bool), "RetrieveColumnAsBoolean" },
            { typeof(byte), "RetrieveColumnAsByte" },
            { typeof(short), "RetrieveColumnAsInt16" },
            { typeof(ushort), "RetrieveColumnAsUInt16" },
            { typeof(int), "RetrieveColumnAsInt32" },
            { typeof(uint), "RetrieveColumnAsUInt32" },
            { typeof(long), "RetrieveColumnAsInt64" },
            { typeof(ulong), "RetrieveColumnAsUInt64" },
            { typeof(float), "RetrieveColumnAsFloat" },
            { typeof(double), "RetrieveColumnAsDouble" },
            { typeof(Guid), "RetrieveColumnAsGuid" },
            { typeof(string), "RetrieveColumnAsString" },
            { typeof(DateTime), "RetrieveColumnAsDateTime" },
            { typeof(TimeSpan), "RetrieveColumnAsTimeSpan" },
        };

        /// <summary>
        /// A mapping of types to ESENT column types.
        /// </summary>
        private static readonly IDictionary<Type, JET_coltyp> coltyps = new Dictionary<Type, JET_coltyp>
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
            { typeof(Guid), JET_coltyp.Binary },
            { typeof(string), JET_coltyp.LongText },
            { typeof(DateTime), JET_coltyp.Currency },
            { typeof(TimeSpan), JET_coltyp.Currency },
        };

        /// <summary>
        /// The SetColumn delegate for this object.
        /// </summary>
        private readonly SetColumnDelegate columnSetter;

        /// <summary>
        /// The RetrieveColumn delegate for this object.
        /// </summary>
        private readonly RetrieveColumnDelegate columnRetriever;

        /// <summary>
        /// The column type for this object.
        /// </summary>
        private readonly JET_coltyp coltyp;

        /// <summary>
        /// Initializes a new instance of the ColumnConverter class.
        /// </summary>
        public ColumnConverter()
        {
            Type underlyingType = IsNullableType(typeof(TColumn)) ? GetUnderlyingType(typeof(TColumn)) : typeof(TColumn);
            if (retrieveColumnMethodNames.ContainsKey(underlyingType))
            {
                this.columnSetter = CreateSetColumnDelegate();
                this.columnRetriever = CreateRetrieveColumnDelegate();
                this.coltyp = coltyps[underlyingType];
            }
            else if (IsSerializable(typeof(TColumn)))
            {
                this.columnSetter = (s, t, c, o) => Api.SerializeObjectToColumn(s, t, c, o);
                this.columnRetriever = (s, t, c) => (TColumn)Api.DeserializeObjectFromColumn(s, t, c);
                this.coltyp = JET_coltyp.LongBinary;
            }
            else
            {
                throw new ArgumentOutOfRangeException("TColumn", typeof(TColumn), "Not supported for SetColumn");                    
            }

            // Compile the new delegates.
            RuntimeHelpers.PrepareDelegate(this.columnSetter);
            RuntimeHelpers.PrepareDelegate(this.columnRetriever);
        }

        /// <summary>
        /// Represents a SetColumn operation.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the value in. An update should be prepared.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        public delegate void SetColumnDelegate(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, TColumn value);

        /// <summary>
        /// Represents a RetrieveColumn operation.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the value from.</param>
        /// <param name="columnid">The column to retrieve.</param>
        /// <returns>The retrieved value.</returns>
        public delegate TColumn RetrieveColumnDelegate(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid);

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
        public SetColumnDelegate ColumnSetter
        {
            get
            {
                return this.columnSetter;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Key column, returning
        /// type <see cref="Type"/>.
        /// </summary>
        public RetrieveColumnDelegate ColumnRetriever
        {
            get
            {
                return this.columnRetriever;
            }
        }

        /// <summary>
        /// Determine if the given type is a nullable type.
        /// </summary>
        /// <param name="t">The type to check.</param>
        /// <returns>True if the type is nullable.</returns>
        private static bool IsNullableType(Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        /// <summary>
        /// Get the type that underlies the nullable type.
        /// </summary>
        /// <param name="t">The nullable type.</param>
        /// <returns>The type that underlies the nullable type.</returns>
        private static Type GetUnderlyingType(Type t)
        {
            Debug.Assert(IsNullableType(t), "Type should be nullable");
            return t.GetGenericArguments()[0];
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
            MemberInfo[] members = type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            return members.Cast<FieldInfo>().All(fieldinfo => IsSerializable(fieldinfo.FieldType));
        }

        /// <summary>
        /// Set a string.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, string value)
        {
            Api.SetColumn(sesid, tableid, columnid, value, Encoding.Unicode, SetColumnGrbit.IntrinsicLV);
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, bool? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, byte? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, short? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, ushort? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, int? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, uint? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, long? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, ulong? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, float? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, double? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable value.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, Guid? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                Api.SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a nullable date time.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, DateTime? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a date time.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, DateTime value)
        {            
            Api.SetColumn(sesid, tableid, columnid, value.Ticks);
        }
        
        /// <summary>
        /// Set a nullable timespan.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, TimeSpan? value)
        {
            if (!SetColumnIfNull(sesid, tableid, columnid, value))
            {
                SetColumn(sesid, tableid, columnid, value.Value);
            }
        }

        /// <summary>
        /// Set a timespan.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to set the value in.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, TimeSpan value)
        {
            Api.SetColumn(sesid, tableid, columnid, value.Ticks);
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
        private static DateTime? RetrieveColumnAsDateTime(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
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
        private static TimeSpan? RetrieveColumnAsTimeSpan(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            long? ticks = Api.RetrieveColumnAsInt64(sesid, tableid, columnid);
            if (ticks.HasValue)
            {
                return new TimeSpan(ticks.Value);
            }

            return null;
        }

        /// <summary>
        /// Set the column to null, if the nullable value is null.
        /// </summary>
        /// <typeparam name="T">The underlying type.</typeparam>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The tablid to set.</param>
        /// <param name="columnid">The column to set.</param>
        /// <param name="value">The nullable value to set.</param>
        /// <returns>
        /// True if the value was null and the column was set to null
        /// .</returns>
        private static bool SetColumnIfNull<T>(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, T? value) where T : struct
        {
            if (!value.HasValue)
            {
                Api.SetColumn(sesid, tableid, columnid, null);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Given a retrieve column delegate that returns a nullable type return a 
        /// delegate that retrieves the column and returns the value of the nullable
        /// type.
        /// </summary>
        /// <param name="arguments">The arguments that the delegate should take.</param>
        /// <param name="method">The retrieve column delegate.</param>
        /// <returns>
        /// A delegate that retrieves the column and returns the value of the nullable type.
        /// </returns>
        private static RetrieveColumnDelegate CreateGetValueDelegate(Type[] arguments, MethodInfo method)
        {
            PropertyInfo value = method.ReturnType.GetProperty("Value");

            DynamicMethod dynamicMethod = new DynamicMethod(
                "RetrieveColumnDynamic",
                MethodAttributes.Static | MethodAttributes.Public,
                CallingConventions.Standard,
                typeof(TColumn),
                arguments,
                typeof(ColumnConverter<TColumn>),
                false);
            ILGenerator generator = dynamicMethod.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Ldarg_2);
            generator.Emit(OpCodes.Call, method);
            LocalBuilder local = generator.DeclareLocal(method.ReturnType);
            generator.Emit(OpCodes.Stloc, local);
            generator.Emit(OpCodes.Ldloca, local);
            generator.Emit(OpCodes.Call, value.GetGetMethod());
            generator.Emit(OpCodes.Ret);

            return (RetrieveColumnDelegate)dynamicMethod.CreateDelegate(typeof(RetrieveColumnDelegate));
        }

        /// <summary>
        /// Get the retrieve column delegate for the type.
        /// </summary>
        /// <returns>The retrieve column delegate for the type.</returns>
        private static RetrieveColumnDelegate CreateRetrieveColumnDelegate()
        {
            // Look for a method called "RetrieveColumnAs{Type}", which will return a
            // nullable version of the type (except for strings, which are are ready 
            // reference types). First look for a private method in this class that
            // takes the appropriate arguments, otherwise a method on the Api class.
            Type underlyingType = IsNullableType(typeof(TColumn)) ? GetUnderlyingType(typeof(TColumn)) : typeof(TColumn);
            string retrieveColumnMethodName = retrieveColumnMethodNames[underlyingType];
            Type[] retrieveColumnArguments = new[] { typeof(JET_SESID), typeof(JET_TABLEID), typeof(JET_COLUMNID) };
            MethodInfo retrieveColumnMethod = typeof(ColumnConverter<TColumn>).GetMethod(
                                                  retrieveColumnMethodName,
                                                  BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.ExactBinding,
                                                  null,
                                                  retrieveColumnArguments,
                                                  null) ?? typeof(Api).GetMethod(
                                                               retrieveColumnMethodName,
                                                               BindingFlags.Static | BindingFlags.Public | BindingFlags.ExactBinding,
                                                               null,
                                                               retrieveColumnArguments,
                                                               null);
            if ((typeof(string) == typeof(TColumn)) || IsNullableType(typeof(TColumn)))
            {
                // Return the string/nullable type.
                return (RetrieveColumnDelegate)Delegate.CreateDelegate(typeof(RetrieveColumnDelegate), retrieveColumnMethod);
            }

            // The retrieve column delegate returns a nullable type. Create a
            // wrapper delegate that returns the value from the nullable type.
            return CreateGetValueDelegate(retrieveColumnArguments, retrieveColumnMethod);
        }

        /// <summary>
        /// Create the set column delegate.
        /// </summary>
        /// <returns>The set column delegate.</returns>
        private static SetColumnDelegate CreateSetColumnDelegate()
        {
            // Look for a method called "SetColumn", which takes a TColumn.
            // First look for a private method in this class that takes the
            // appropriate arguments, otherwise a method on the Api class.
            const string SetColumnMethodName = "SetColumn";
            Type[] setColumnArguments = new[] { typeof(JET_SESID), typeof(JET_TABLEID), typeof(JET_COLUMNID), typeof(TColumn) };
            MethodInfo setColumnMethod = typeof(ColumnConverter<TColumn>).GetMethod(
                                             SetColumnMethodName,
                                             BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.ExactBinding,
                                             null,
                                             setColumnArguments,
                                             null) ?? typeof(Api).GetMethod(
                                                          SetColumnMethodName,
                                                          BindingFlags.Static | BindingFlags.Public | BindingFlags.ExactBinding,
                                                          null,
                                                          setColumnArguments,
                                                          null);
            return (SetColumnDelegate)Delegate.CreateDelegate(typeof(SetColumnDelegate), setColumnMethod);
        }
    }
}
