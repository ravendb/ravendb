using System;
using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public static class PowerBIConfig
    {
        // Note: PowerBI Desktop uses and ships with NpgSQL 4.0.10 and does not recommend 4.1 and up. https://docs.microsoft.com/en-us/power-query/connectors/postgresql

        public static readonly string TableSchemaQuery = "select\n    pkcol.COLUMN_NAME as PK_COLUMN_NAME,\n    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,\n    fkcol.TABLE_NAME AS FK_TABLE_NAME,\n    fkcol.COLUMN_NAME as FK_COLUMN_NAME,\n    fkcol.ORDINAL_POSITION as ORDINAL,\n    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME";
        public static readonly string TableSchemaSecondaryQuery = "select\n    pkcol.TABLE_SCHEMA AS PK_TABLE_SCHEMA,\n    pkcol.TABLE_NAME AS PK_TABLE_NAME,\n    pkcol.COLUMN_NAME as PK_COLUMN_NAME,\n    fkcol.COLUMN_NAME as FK_COLUMN_NAME,\n    fkcol.ORDINAL_POSITION as ORDINAL,\n    fkcon.CONSTRAINT_SCHEMA ";
        public static readonly string ConstraintsQuery = "select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY\nfrom INFORMATION_SCHEMA.table_constraints i inner join INFORMATION_SCHEMA.key_column_usage ii on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and i.TABLE_SCHEMA = ii.TABLE_SCHEMA and i.TABLE_NAME = ii.TABLE_NAME";
        public static readonly string CharacterSetsQuery = "select character_set_name from INFORMATION_SCHEMA.character_sets";

        public static readonly PgTable TableSchemaResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("pk_column_name", 0, PgName.Default, PgFormat.Binary),
                new PgColumn("fk_table_schema", 1, PgName.Default, PgFormat.Binary),
                new PgColumn("fk_table_name", 2, PgName.Default, PgFormat.Binary),
                new PgColumn("fk_column_name", 3, PgName.Default, PgFormat.Binary),
                new PgColumn("ordinal", 4, PgInt4.Default, PgFormat.Binary),
                new PgColumn("fk_name", 5, PgName.Default, PgFormat.Binary),
            }
        };

        public static readonly PgTable TableSchemaSecondaryResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("pk_table_schema", 0, PgName.Default, PgFormat.Binary),
                new PgColumn("pk_table_name", 1, PgName.Default, PgFormat.Binary),
                new PgColumn("pk_column_name", 2, PgName.Default, PgFormat.Binary),
                new PgColumn("fk_column_name", 3, PgName.Default, PgFormat.Binary),
                new PgColumn("ordinal", 4, PgInt4.Default, PgFormat.Binary),
                new PgColumn("fk_name", 5, PgName.Default, PgFormat.Binary),
            }
        };

        public static readonly PgTable ConstraintsResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("index_name", 0, PgText.Default, PgFormat.Binary),
                new PgColumn("column_name", 1, PgName.Default, PgFormat.Binary),
                new PgColumn("ordinal_position", 2, PgInt4.Default, PgFormat.Binary),
                new PgColumn("primary_key", 3, PgText.Default, PgFormat.Binary),
            }
        };

        public static readonly PgTable CharacterSetsResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("character_set_name", 0, PgName.Default, PgFormat.Text),
            },
            Data = new List<PgDataRow>
            {
                new()
                {
                    ColumnData = new ReadOnlyMemory<byte>?[]
                    {
                        PgName.Default.ToBytes("UTF8", PgFormat.Text)
                    }
                },
            }
        };
    }
}
