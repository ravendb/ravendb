//-----------------------------------------------------------------------
// <copyright file="ColumnDefinition.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Define an ESE column. Used when adding a column.
    /// </summary>
    public struct ColumnDefinition
    {
        /// <summary>
        /// Initializes a new instance of the ColumnDefinition struct.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="type">The column type.</param>
        public ColumnDefinition(string name, ColumnType type) : this()
        {            
            this.Name = name;
            this.Type = type;
        }

        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the type of the column.
        /// </summary>
        public ColumnType Type { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether the column is an autoincrement column.
        /// </summary>
        public bool IsAutoincrement { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the column is a non-NULL column.
        /// </summary>
        public bool IsNotNull { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the column is a version column.
        /// </summary>
        public bool IsVersion { get; set; }

        /// <summary>
        /// Gets or sets a value giving the maximum length of the column.
        /// </summary>
        public int MaxSize { get; set; }

        /// <summary>
        /// Gets or sets a value specifying the default value of the column.
        /// </summary>
        public object DefaultValue { get; set; }

        /// <summary>
        /// Sets the column to be autoincrement.
        /// </summary>
        /// <returns>The column definition.</returns>
        public ColumnDefinition AsAutoincrement()
        {
            ColumnDefinition newDefinition = this.Clone();
            newDefinition.IsAutoincrement = true;
            return newDefinition;
        }

        /// <summary>
        /// Disallows null values for the column.
        /// </summary>
        /// <returns>The column definition.</returns>
        public ColumnDefinition MustBeNonNull()
        {
            ColumnDefinition newDefinition = this.Clone();
            newDefinition.IsNotNull = true;
            return newDefinition;
        }

        /// <summary>
        /// Sets the column to be a Version column.
        /// </summary>
        /// <returns>The column definition.</returns>
        public ColumnDefinition AsVersion()
        {
            ColumnDefinition newDefinition = this.Clone();
            newDefinition.IsVersion = true;
            return newDefinition;
        }

        /// <summary>
        /// Sets the maximum length of the column.
        /// </summary>
        /// <param name="length">The maximum length of the column, in bytes.</param>
        /// <returns>The column definition.</returns>
        public ColumnDefinition WithMaxSize(int length)
        {
            ColumnDefinition newDefinition = this.Clone();
            newDefinition.MaxSize = length;
            return newDefinition;
        }

        /// <summary>
        /// Sets the default value of the column.
        /// </summary>
        /// <param name="value">The default value of the column.</param>
        /// <returns>The column definition.</returns>
        public ColumnDefinition WithDefaultValue(object value)
        {
            ColumnDefinition newDefinition = this.Clone();
            newDefinition.DefaultValue = value;
            return newDefinition;
        }

        /// <summary>
        /// Create the column for this definition in the specified table.
        /// </summary>
        /// <param name="cursor">
        /// The cursor to create the column on.
        /// </param>
        internal void CreateColumn(Cursor cursor)
        {
            var interopConversion = Dependencies.Container.Resolve<InteropConversion>();

            JET_COLUMNDEF columndef = interopConversion.CreateColumndefFromColumnDefinition(this);

            JET_COLUMNID ignored;
            if (null != this.DefaultValue)
            {
                var dataConversion = Dependencies.Container.Resolve<DataConversion>();
                object defaultValueObject = dataConversion.ConvertToObject[this.Type](this.DefaultValue);
                byte[] defaultValue = dataConversion.ConvertObjectToBytes[this.Type](defaultValueObject);
                cursor.AddColumn(this.Name, columndef, defaultValue, out ignored);
            }
            else
            {
                cursor.AddColumn(this.Name, columndef, null, out ignored);
            }
        }

        /// <summary>
        /// Returns a copy of the ColumnDefinition.
        /// </summary>
        /// <returns>A copy of the current definition.</returns>
        private ColumnDefinition Clone()
        {
            var clone = new ColumnDefinition(this.Name, this.Type)
                {
                    IsAutoincrement = this.IsAutoincrement,
                    IsNotNull = this.IsNotNull,
                    IsVersion = this.IsVersion,
                    MaxSize = this.MaxSize,
                    DefaultValue = this.DefaultValue
                };
            return clone;
        }
    }
}