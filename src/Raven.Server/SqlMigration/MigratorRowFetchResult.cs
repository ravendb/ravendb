using System.Collections.Generic;

namespace Raven.Server.SqlMigration
{
    public enum RowFetchMode
    {
        /// <summary>Pull the first N rows of the table, ordered by the supplied primary-key columns for determinism.</summary>
        First,

        /// <summary>Pull the single row whose primary-key values match the supplied list.</summary>
        ByPrimaryKey
    }

    /// <summary>
    /// Raw rows returned by <see cref="IDatabaseDriver.FetchRowsAsync"/>. The arrays in <see cref="Rows"/>
    /// are positional and the <see cref="ColumnNames"/> entries at the same index name them — same shape
    /// the CDC sink uses internally (<c>CdcSinkTableProcessor.SourceColumnNames</c> + per-row <c>object[]</c>).
    /// </summary>
    public sealed class MigratorRowFetchResult
    {
        public string[] ColumnNames { get; init; }

        public List<object[]> Rows { get; init; }
    }
}
