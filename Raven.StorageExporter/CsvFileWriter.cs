using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.StorageExporter
{
    public class CsvFileWriter : IDisposable
    {
        public CsvFileWriter(string fullPath)
        {
            writer = File.CreateText(fullPath);
        }

        public void WriteHeaders(IEnumerable<string> csvHeaders)
        {
            writer.WriteLine(string.Join(",", csvHeaders));
            numberOfColumns = csvHeaders.Count();
        }

        public void WriteCsvColumnValue(string columnValue, bool escape = false)
        {            
            var writeValue = columnValue;
            if (escape)
            {
                columnStringBuilder.Clear();
                columnStringBuilder.Append("\"").Append(columnValue.Replace("\r\n", "").Replace("\"", "\"\"")).Append("\"");
                writeValue = columnStringBuilder.ToString();
            }
            rowStringBuilder.Append(writeValue).Append(",");
            currentColumn++;
            if (currentColumn == numberOfColumns)
            {
                rowStringBuilder.Remove(rowStringBuilder.Length - 1, 1);
                currentColumn = 0;
                writer.WriteLine(rowStringBuilder.ToString());
                rowStringBuilder.Clear();
            }
        }
        public void Dispose()
        {
            if (writer != null) writer.Close();
        }

        private int numberOfColumns;
        private int currentColumn;
        private StringBuilder rowStringBuilder = new StringBuilder();
        private StringBuilder columnStringBuilder = new StringBuilder();
        private StreamWriter writer;
    }
}
