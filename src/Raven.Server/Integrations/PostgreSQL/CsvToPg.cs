using System;
using System.Collections.Generic;
using Microsoft.VisualBasic.FileIO;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL
{
    public static class CsvToPg
    {
        public static PgTable Convert(string filePath, Dictionary<string, PgColumn> columns)
        {
            var table = new PgTable();
            using (var parser = new TextFieldParser(filePath))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");

                var isFirstRow = true;
                while (!parser.EndOfData)
                {
                    var fields = parser.ReadFields();
                    if (fields != null)
                    {
                        if (isFirstRow)
                        {
                            isFirstRow = false;
                            HandleColumns(fields, columns, ref table);
                            continue;
                        }

                        var row = new ReadOnlyMemory<byte>?[fields.Length];

                        for (var index = 0; index < fields.Length; index++)
                        {
                            var field = fields[index];

                            var obj = field switch
                            {
                                "NULL" => null,
                                "False" => false,
                                "True" => true,
                                _ => table.Columns[index].PgType.FromString(field)
                            };

                            if (obj == null)
                                row[index] = null;
                            else
                                row[index] = table.Columns[index].PgType.ToBytes(obj, table.Columns[index].FormatCode);
                        }

                        // TODO: Create a constructor for PgDataRow
                        table.Data.Add(new PgDataRow
                        {
                            ColumnData = row
                        });
                    }
                }
            }

            return table;
        }

        private static void HandleColumns(string[] fields, IReadOnlyDictionary<string, PgColumn> columns, ref PgTable pgTable)
        {
            foreach (var columnName in fields)
            {
                if (columns.TryGetValue(columnName, out var columnValue) == false)
                {
                    throw new Exception("CSV contained a column that wasn't found in the provided columns dictionary.");
                }

                pgTable.Columns.Add(columnValue);
            }
        }
    }
}
