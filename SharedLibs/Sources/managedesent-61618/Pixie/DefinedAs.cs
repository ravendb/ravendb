//-----------------------------------------------------------------------
// <copyright file="DefinedAs.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Static methods for meta-data creation. These provide a more fluent
    /// syntax.
    /// </summary>
    public class DefinedAs
    {
        /// <summary>
        /// Create a new ColumnDefinition for a Bool column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Bool column with the specified name.
        /// </returns>
        public static ColumnDefinition BoolColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Bool);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Byte column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Byte column with the specified name.
        /// </returns>
        public static ColumnDefinition ByteColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Byte);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Short column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Short column with the specified name.
        /// </returns>
        public static ColumnDefinition Int16Column(string name)
        {
            return new ColumnDefinition(name, ColumnType.Int16);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a UShort column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a UShort column with the specified name.
        /// </returns>
        public static ColumnDefinition UInt16Column(string name)
        {
            return new ColumnDefinition(name, ColumnType.UInt16);
        }

        /// <summary>
        /// Create a new ColumnDefinition for an Integer column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for an Integer column with the specified name.
        /// </returns>
        public static ColumnDefinition Int32Column(string name)
        {
            return new ColumnDefinition(name, ColumnType.Int32);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a UInt column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a UInt column with the specified name.
        /// </returns>
        public static ColumnDefinition UInt32Column(string name)
        {
            return new ColumnDefinition(name, ColumnType.UInt32);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Long column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Long column with the specified name.
        /// </returns>
        public static ColumnDefinition Int64Column(string name)
        {
            return new ColumnDefinition(name, ColumnType.Int64);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Float column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Float column with the specified name.
        /// </returns>
        public static ColumnDefinition FloatColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Float);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Double column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Double column with the specified name.
        /// </returns>
        public static ColumnDefinition DoubleColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Double);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a DateTime column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a DateTime column with the specified name.
        /// </returns>
        public static ColumnDefinition DateTimeColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.DateTime);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Guid column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Guid column with the specified name.
        /// </returns>
        public static ColumnDefinition GuidColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Guid);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a Binary column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a Binary column with the specified name.
        /// </returns>
        public static ColumnDefinition BinaryColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Binary);
        }

        /// <summary>
        /// Create a new ColumnDefinition for a text column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for a text column with the specified name.
        /// </returns>
        public static ColumnDefinition TextColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.Text);
        }

        /// <summary>
        /// Create a new ColumnDefinition for an ASCII text column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// A column definition for an ASCII text column with the specified name.
        /// </returns>
        public static ColumnDefinition AsciiTextColumn(string name)
        {
            return new ColumnDefinition(name, ColumnType.AsciiText);
        }

        /// <summary>
        /// Create a new IndexDefinition for the named index.
        /// </summary>
        /// <param name="name">The name of the index.</param>
        /// <returns>
        /// A index definition for the index.
        /// </returns>
        public static IndexDefinition Index(string name)
        {
            return new IndexDefinition(name);
        }

        /// <summary>
        /// Create a new IndexDefinition for the named index. The
        /// index will be the primary index.
        /// </summary>
        /// <param name="name">The name of the index.</param>
        /// <returns>
        /// A index definition for the index.
        /// </returns>
        public static IndexDefinition PrimaryIndex(string name)
        {
            return new IndexDefinition(name).AsPrimary();
        }

        /// <summary>
        /// Create a new IndexDefinition for the named index. The
        /// index will be a unique index.
        /// </summary>
        /// <param name="name">The name of the index.</param>
        /// <returns>
        /// A index definition for the index.
        /// </returns>
        public static IndexDefinition UniqueIndex(string name)
        {
            return new IndexDefinition(name).AsUnique();
        }
    }
}