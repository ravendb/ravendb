//-----------------------------------------------------------------------
// <copyright file="types.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Runtime.InteropServices;

    /// <summary>
    /// A JET_INSTANCE contains a handle to the instance of the database to use for calls to the JET Api.
    /// </summary>
    public struct JET_INSTANCE : IEquatable<JET_INSTANCE>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal IntPtr Value;

        /// <summary>
        /// Gets a null JET_INSTANCE.
        /// </summary>
        public static JET_INSTANCE Nil
        {
            [DebuggerStepThrough]
            get { return new JET_INSTANCE(); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_INSTANCE
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_INSTANCE lhs, JET_INSTANCE rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_INSTANCE
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_INSTANCE lhs, JET_INSTANCE rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_INSTANCE(0x{0:x})", this.Value.ToInt64());
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToInt64().ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_INSTANCE)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_INSTANCE other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// A JET_SESID contains a handle to the session to use for calls to the JET Api.
    /// </summary>
    public struct JET_SESID : IEquatable<JET_SESID>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal IntPtr Value;

        /// <summary>
        /// Gets a null JET_SESID.
        /// </summary>
        public static JET_SESID Nil
        {
            [DebuggerStepThrough]
            get { return new JET_SESID(); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_SESID
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_SESID lhs, JET_SESID rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_SESID
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_SESID lhs, JET_SESID rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_SESID(0x{0:x})", this.Value.ToInt64());
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToInt64().ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_SESID)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_SESID other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// A JET_TABLEID contains a handle to the database cursor to use for a call to the JET Api.
    /// A cursor can only be used with the session that was used to open that cursor.
    /// </summary>
    public struct JET_TABLEID : IEquatable<JET_TABLEID>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal IntPtr Value;

        /// <summary>
        /// Gets a null JET_TABLEID.
        /// </summary>
        public static JET_TABLEID Nil
        {
            [DebuggerStepThrough]
            get { return new JET_TABLEID(); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_TABLEID
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_TABLEID lhs, JET_TABLEID rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_TABLEID
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_TABLEID lhs, JET_TABLEID rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_TABLEID(0x{0:x})", this.Value.ToInt64());
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToInt64().ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_TABLEID)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_TABLEID other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// A JET_DBID contains the handle to the database. A database handle is used to manage the
    /// schema of a database. It can also be used to manage the tables inside of that database.
    /// </summary>
    public struct JET_DBID : IEquatable<JET_DBID>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal uint Value;

        /// <summary>
        /// Gets a null JET_DBID.
        /// </summary>
        public static JET_DBID Nil
        {
            get
            {
                return new JET_DBID { Value = 0xffffffff };
            }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_DBID
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_DBID lhs, JET_DBID rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_DBID
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_DBID lhs, JET_DBID rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_DBID({0})", this.Value);
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_DBID)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_DBID other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// A JET_COLUMNID identifies a column within a table.
    /// </summary>
    public struct JET_COLUMNID : IEquatable<JET_COLUMNID>, IComparable<JET_COLUMNID>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal uint Value;

        /// <summary>
        /// Gets a null JET_COLUMNID.
        /// </summary>
        public static JET_COLUMNID Nil
        {
            [DebuggerStepThrough]
            get { return new JET_COLUMNID(); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_COLUMNID
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_COLUMNID lhs, JET_COLUMNID rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_COLUMNID
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_COLUMNID lhs, JET_COLUMNID rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Determine whether one columnid is before another columnid.
        /// </summary>
        /// <param name="lhs">The first columnid to compare.</param>
        /// <param name="rhs">The second columnid to compare.</param>
        /// <returns>True if lhs comes before rhs.</returns>
        public static bool operator <(JET_COLUMNID lhs, JET_COLUMNID rhs)
        {
            return lhs.CompareTo(rhs) < 0;
        }

        /// <summary>
        /// Determine whether one columnid is after another columnid.
        /// </summary>
        /// <param name="lhs">The first columnid to compare.</param>
        /// <param name="rhs">The second columnid to compare.</param>
        /// <returns>True if lhs comes after rhs.</returns>
        public static bool operator >(JET_COLUMNID lhs, JET_COLUMNID rhs)
        {
            return lhs.CompareTo(rhs) > 0;
        }

        /// <summary>
        /// Determine whether one columnid is before or equal to
        /// another columnid.
        /// </summary>
        /// <param name="lhs">The first columnid to compare.</param>
        /// <param name="rhs">The second columnid to compare.</param>
        /// <returns>True if lhs comes before or is equal to rhs.</returns>
        public static bool operator <=(JET_COLUMNID lhs, JET_COLUMNID rhs)
        {
            return lhs.CompareTo(rhs) <= 0;
        }

        /// <summary>
        /// Determine whether one columnid is after or equal to
        /// another columnid.
        /// </summary>
        /// <param name="lhs">The first columnid to compare.</param>
        /// <param name="rhs">The second columnid to compare.</param>
        /// <returns>True if lhs comes after or is equal to rhs.</returns>
        public static bool operator >=(JET_COLUMNID lhs, JET_COLUMNID rhs)
        {
            return lhs.CompareTo(rhs) >= 0;
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_COLUMNID(0x{0:x})", this.Value);
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_COLUMNID)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_COLUMNID other)
        {
            return this.Value.Equals(other.Value);
        }

        /// <summary>
        /// Compares this columnid to another columnid and determines
        /// whether this instance is before, the same as or after the other
        /// instance.
        /// </summary>
        /// <param name="other">The columnid to compare to the current instance.</param>
        /// <returns>
        /// A signed number indicating the relative positions of this instance and the value parameter.
        /// </returns>
        public int CompareTo(JET_COLUMNID other)
        {
            return this.Value.CompareTo(other.Value);
        }
    }

    /// <summary>
    /// A JET_OSSNAPID contains a handle to a snapshot of a database.
    /// </summary>
    public struct JET_OSSNAPID : IEquatable<JET_OSSNAPID>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal IntPtr Value;

        /// <summary>
        /// Gets a null JET_OSSNAPID.
        /// </summary>
        public static JET_OSSNAPID Nil
        {
            [DebuggerStepThrough]
            get { return new JET_OSSNAPID(); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_OSSNAPID
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_OSSNAPID lhs, JET_OSSNAPID rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_OSSNAPID
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_OSSNAPID lhs, JET_OSSNAPID rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_OSSNAPID(0x{0:x})", this.Value.ToInt64());
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToInt64().ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_OSSNAPID)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_OSSNAPID other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// A JET_HANDLE contains a generic handle.
    /// </summary>
    public struct JET_HANDLE : IEquatable<JET_HANDLE>, IFormattable
    {
        /// <summary>
        /// The native value.
        /// </summary>
        internal IntPtr Value;

        /// <summary>
        /// Gets a null JET_HANDLE.
        /// </summary>
        public static JET_HANDLE Nil
        {
            [DebuggerStepThrough]
            get { return new JET_HANDLE(); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_HANDLE
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_HANDLE lhs, JET_HANDLE rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_HANDLE
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_HANDLE lhs, JET_HANDLE rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_HANDLE(0x{0:x})", this.Value.ToInt64());
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToInt64().ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_HANDLE)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_HANDLE other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// Local storage for an ESENT handle. Used by <see cref="Api.JetGetLS"/>
    /// and <see cref="Api.JetSetLS"/>.
    /// </summary>
    public struct JET_LS : IEquatable<JET_LS>, IFormattable
    {
        /// <summary>
        /// The null handle.
        /// </summary>
        public static readonly JET_LS Nil = new JET_LS { Value = new IntPtr(~0) };

        /// <summary>
        /// Gets or sets the value of the handle.
        /// </summary>
        public IntPtr Value { get; set; }

        /// <summary>
        /// Determines whether two specified instances of JET_LS
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_LS lhs, JET_LS rhs)
        {
            return lhs.Value == rhs.Value;
        }

        /// <summary>
        /// Determines whether two specified instances of JET_LS
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_LS lhs, JET_LS rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_LS(0x{0:x})", this.Value.ToInt64());
        }

        /// <summary>
        /// Formats the value of the current instance using the specified format.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> containing the value of the current instance in the specified format.
        /// </returns>
        /// <param name="format">The <see cref="T:System.String"/> specifying the format to use.
        /// -or- 
        /// null to use the default format defined for the type of the <see cref="T:System.IFormattable"/> implementation. 
        /// </param>
        /// <param name="formatProvider">The <see cref="T:System.IFormatProvider"/> to use to format the value.
        /// -or- 
        /// null to obtain the numeric format information from the current locale setting of the operating system. 
        /// </param>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return String.IsNullOrEmpty(format) || "G" == format ? this.ToString() : this.Value.ToInt64().ToString(format, formatProvider);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_LS)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_LS other)
        {
            return this.Value.Equals(other.Value);
        }
    }

    /// <summary>
    /// Holds an index ID. An index ID is a hint that is used to accelerate the
    /// selection of the current index using JetSetCurrentIndex. It is most
    /// useful when there is a very large number of indexes over a table. The
    /// index ID can be retrieved using JetGetIndexInfo or JetGetTableIndexInfo.
    /// </summary>
    /// <remarks>
    /// The Pack attribute is necessary because the C++ version is defined as
    /// a byte array. If the C# compiler inserts the usual padding between the IntPtr
    /// and uint, then the structure ends up too large.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public struct JET_INDEXID : IEquatable<JET_INDEXID>
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        internal uint CbStruct;

        /// <summary>
        /// Internal use only.
        /// </summary>
        internal IntPtr IndexId1;

        /// <summary>
        /// Internal use only.
        /// </summary>
        internal uint IndexId2;

        /// <summary>
        /// Internal use only.
        /// </summary>
        internal uint IndexId3;

        /// <summary>
        /// The size of a JET_INDEXID structure.
        /// </summary>
        private static readonly uint sizeOfIndexId = (uint)Marshal.SizeOf(typeof(JET_INDEXID));

        /// <summary>
        /// Gets the size of a JET_INDEXINDEXID structure.
        /// </summary>
        internal static uint SizeOfIndexId
        {
            [DebuggerStepThrough]
            get { return sizeOfIndexId; }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_INDEXID
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_INDEXID lhs, JET_INDEXID rhs)
        {
            return lhs.Equals(rhs);
        }

        /// <summary>
        /// Determines whether two specified instances of JET_INDEXID
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_INDEXID lhs, JET_INDEXID rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_INDEXID)obj);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_INDEXID(0x{0:x}:0x{1:x}:0x{2:x})",
                this.IndexId1,
                this.IndexId2,
                this.IndexId3);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.CbStruct.GetHashCode()
                   ^ this.IndexId1.GetHashCode()
                   ^ this.IndexId2.GetHashCode()
                   ^ this.IndexId3.GetHashCode();
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_INDEXID other)
        {
            return this.CbStruct == other.CbStruct
                   && this.IndexId1 == other.IndexId1
                   && this.IndexId2 == other.IndexId2
                   && this.IndexId3 == other.IndexId3;
        }        
    }
}
