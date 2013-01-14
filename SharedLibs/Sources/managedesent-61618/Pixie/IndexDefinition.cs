//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------


namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Define an ESE index. Used when adding an index.
    /// </summary>
    public struct IndexDefinition
    {
        /// <summary>
        /// Initializes a new instance of the IndexDefinition struct.
        /// </summary>
        /// <param name="name">Name of the index.</param>
        public IndexDefinition(string name)
            : this()
        {
            this.Name = name;
        }

        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether the index is unique.
        /// </summary>
        /// <remarks>
        /// This is CreateIndexGrbit.IndexUnique
        /// </remarks>
        public bool IsUnique { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the index is the primary index. 
        /// A table can only have one primary index, the primary index must be unique
        /// and the table has to be empty when the primary index is aded.
        /// </summary>
        /// <remarks>
        /// This is CreateIndexGrbit.IndexPrimary
        /// </remarks>
        public bool IsPrimary { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the index allows entries which
        /// contain NULL columns.
        /// </summary>
        /// <remarks>
        /// This is CreateIndexGrbit.IndexDisallowNull
        /// </remarks>
        public bool RejectNullColumns { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether records where the index key
        /// evaluates to null should be ignored.
        /// </summary>
        /// <remarks>
        /// This is CreateIndexGrbit.IndexIgnoreNull
        /// </remarks>
        public bool IgnoreRecordsWithNullKey { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether records where any indexed
        /// column evaluates to null should be ignored.
        /// </summary>
        /// <remarks>
        /// This is CreateIndexGrbit.IndexIgnoreAnyNull
        /// </remarks>
        public bool IgnoreRecordsWithNullColumn { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether records where the first indexed
        /// column evaluates to null should be ignored.
        /// </summary>
        /// <remarks>
        /// This is CreateIndexGrbit.IndexIgnoreFirstNull
        /// </remarks>
        public bool IgnoreRecordsWhereFirstColumnIsNull { get; set; }

        /// <summary>
        /// Sets the index to be the primary index.
        /// </summary>
        /// <returns>An index definition.</returns>
        public IndexDefinition AsPrimary()
        {
            IndexDefinition newDefinition = this.Clone();
            newDefinition.IsPrimary = true;
            newDefinition.IsUnique = true;
            return newDefinition;
        }

        /// <summary>
        /// Sets the index to be unique.
        /// </summary>
        /// <returns>An index definition.</returns>
        public IndexDefinition AsUnique()
        {
            IndexDefinition newDefinition = this.Clone();
            newDefinition.IsUnique = true;
            return newDefinition;
        }

        /// <summary>
        /// Sets the index to ignore records whose index key evaluates to null.
        /// </summary>
        /// <returns>An index definition.</returns>
        public IndexDefinition IgnoringRecordsWithNullKey()
        {
            IndexDefinition newDefinition = this.Clone();
            newDefinition.IgnoreRecordsWithNullKey = true;
            return newDefinition;
        }

        /// <summary>
        /// Sets the index to ignore records where any indexed column is null.
        /// </summary>
        /// <returns>An index definition.</returns>
        public IndexDefinition IgnoringRecordsWithNullColumn()
        {
            IndexDefinition newDefinition = this.Clone();
            newDefinition.IgnoreRecordsWithNullColumn = true;
            return newDefinition;
        }

        /// <summary>
        /// Sets the index to ignore records where any indexed column is null.
        /// </summary>
        /// <returns>An index definition.</returns>
        public IndexDefinition IgnoringRecordsWhereFirstColumnIsNull()
        {
            IndexDefinition newDefinition = this.Clone();
            newDefinition.IgnoreRecordsWhereFirstColumnIsNull = true;
            return newDefinition;
        }

        /// <summary>
        /// Create the index for this definition in the specified table.
        /// </summary>
        /// <param name="cursor">
        /// The cursor to create the column on.
        /// </param>
        internal void CreateIndex(Cursor cursor)
        {
            //var interopConversion = Dependencies.Container.Resolve<InteropConversion>();

            //JET_INDEXDEF columndef = interopConversion.CreateColumndefFromIndexDefinition(this);

            //JET_COLUMNID ignored;
            //if (null != this.DefaultValue)
            //{
            //    var dataConversion = Dependencies.Container.Resolve<DataConversion>();
            //    object defaultValueObject = dataConversion.ConvertToObject[this.Type](this.DefaultValue);
            //    byte[] defaultValue = dataConversion.ConvertObjectToBytes[this.Type](defaultValueObject);
            //    cursor.AddColumn(this.Name, columndef, defaultValue, out ignored);
            //}
            //else
            //{
            //    cursor.AddColumn(this.Name, columndef, null, out ignored);
            //}
        }

        /// <summary>
        /// Returns a copy of the IndexDefinition.
        /// </summary>
        /// <returns>A copy of the current definition.</returns>
        private IndexDefinition Clone()
        {
            var clone = new IndexDefinition(this.Name)
            {
                IsPrimary =  this.IsPrimary,
                IsUnique = this.IsUnique,
                IgnoreRecordsWhereFirstColumnIsNull = this.IgnoreRecordsWhereFirstColumnIsNull,
                IgnoreRecordsWithNullColumn = this.IgnoreRecordsWithNullColumn,
                IgnoreRecordsWithNullKey = this.IgnoreRecordsWithNullKey,
            };
            return clone;
        }
    }
}