using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CsvHelper;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    internal static class CatalogCsvLoader
    {
        public static List<object[]> Load(string fileName, IReadOnlyList<PgVirtualColumn> columns)
        {
            var resourceName = "Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables.Data." + fileName;
            var assembly = typeof(CatalogCsvLoader).Assembly;
            using var stream = assembly.GetManifestResourceStream(resourceName)
                               ?? throw new FileNotFoundException($"Catalog CSV resource not found: {resourceName}");

            using var csv = CsvReader.Create(stream, new CsvReaderOptions { Separator = ',', DetectSeparatorHint = false });

            var header = csv.Headers.ToArray();
            if (header.Length == 0)
                throw new InvalidDataException($"Catalog CSV {fileName} has no header row.");
            var columnIndex = new int[columns.Count];
            for (int i = 0; i < columns.Count; i++)
            {
                columnIndex[i] = Array.FindIndex(header, h => string.Equals(h, columns[i].Name, StringComparison.OrdinalIgnoreCase));
                if (columnIndex[i] < 0)
                    throw new InvalidDataException($"Column '{columns[i].Name}' missing from CSV header in {fileName}.");
            }

            var rows = new List<object[]>();
            while (csv.Read())
            {
                var row = new object[columns.Count];
                for (int i = 0; i < columns.Count; i++)
                    row[i] = ParseCell(csv.GetString(columnIndex[i]), columns[i].PgType);
                rows.Add(row);
            }

            return rows;
        }

        private static object ParseCell(string raw, PgType pgType)
        {
            if (raw == null || raw == "NULL")
                return null;

            return raw switch
            {
                "False" => false,
                "True" => true,
                _ => pgType.FromString(raw),
            };
        }
    }
}
