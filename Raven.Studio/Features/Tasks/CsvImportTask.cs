using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Kent.Boogaart.KBCsv;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Util;
using Raven.Json.Linq;
using Path = System.IO.Path;

namespace Raven.Studio.Features.Tasks
{
    public class CsvImportTask : DatabaseTask
    {
        const int BatchSize = 512;

        public CsvImportTask(IAsyncDatabaseCommands databaseCommands,  string databaseName) : base(databaseCommands, "CSV Import", databaseName)
        {
        }

        protected override async Task<DatabaseTaskOutcome> RunImplementation()
        {
            var openFile = new OpenFileDialog
            {
                Filter = "csv|*.csv"
            };

            if (openFile.ShowDialog() != true)
                return DatabaseTaskOutcome.Abandoned;

            Report(string.Format("Importing from {0}", openFile.File.Name));

            using (var streamReader = openFile.File.OpenText())
            {
                await ImportAsync(streamReader, openFile.File.Name);
            }

            return DatabaseTaskOutcome.Succesful;
        }

        private async Task ImportAsync(StreamReader streamReader, string name)
        {
            using (var csvReader = new CsvReader(streamReader))
            {
                var header = csvReader.ReadHeaderRecord();
                var entity =
                    Inflector.Pluralize(CSharpClassName.ConvertToValidClassName(Path.GetFileNameWithoutExtension(name)));
                if (entity.Length > 0 && char.IsLower(entity[0]))
                    entity = char.ToUpper(entity[0]) + entity.Substring(1);

                var totalCount = 0;
                var batch = new List<RavenJObject>();
                var columns = header.Values.Where(x => x.StartsWith("@") == false).ToArray();

				batch.Clear();
                foreach (var record in csvReader.DataRecords)
                {
                    var document = new RavenJObject();
                    string id = null;
                    RavenJObject metadata = null;
                    foreach (var column in columns)
                    {
                        if (string.IsNullOrEmpty(column))
                            continue;

                        if (string.Equals("id", column, StringComparison.OrdinalIgnoreCase))
                        {
                            id = record[column];
                        }
                        else if (string.Equals(Constants.RavenEntityName, column, StringComparison.OrdinalIgnoreCase))
                        {
                            metadata = metadata ?? new RavenJObject();
                            metadata[Constants.RavenEntityName] = record[column];
                            id = id ?? record[column] + "/";
                        }
                        else if (string.Equals(Constants.RavenClrType, column, StringComparison.OrdinalIgnoreCase))
                        {
                            metadata = metadata ?? new RavenJObject();
                            metadata[Constants.RavenClrType] = record[column];
                            id = id ?? record[column] + "/";
                        }
                        else
                        {
                            document[column] = SetValueInDocument(record[column]);
                        }
                    }

                    metadata = metadata ?? new RavenJObject {{"Raven-Entity-Name", entity}};
                    document.Add("@metadata", metadata);
                    metadata.Add("@id", id ?? Guid.NewGuid().ToString());

                    batch.Add(document);
                    totalCount++;

                    if (batch.Count >= BatchSize)
                    {
                        await FlushBatch(batch);
						batch.Clear();
                    }
                }

                if (batch.Count > 0)
                {
                    await FlushBatch(batch);
                }

                Report(String.Format("Imported {0:#,#;;0} documents", totalCount));
            }
        }

        private static RavenJToken SetValueInDocument(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            var ch = value[0];
            if (ch == '[' || ch == '{')
            {
                try
                {
                    return RavenJToken.Parse(value);
                }
                catch (Exception)
                {
                    // ignoring failure to parse, will proceed to insert as a string value
                }
            }
            else if (char.IsDigit(ch) || ch == '-' || ch == '.')
            {
                // maybe it is a number?
                long longResult;
                if (long.TryParse(value, out longResult))
                {
                    return longResult;
                }

                decimal decimalResult;
                if (decimal.TryParse(value, out decimalResult))
                {
                    return decimalResult;
                }
            }
            else if (ch == '"' && value.Length > 1 && value[value.Length - 1] == '"')
            {
                return value.Substring(1, value.Length - 2);
            }

            return value;
        }

        async Task FlushBatch(ICollection<RavenJObject> batch)
        {
            var sw = Stopwatch.StartNew();

            var commands = (from doc in batch
                            let metadata = doc.Value<RavenJObject>("@metadata")
                            let removal = doc.Remove("@metadata")
                            select new PutCommandData
                            {
                                Metadata = metadata,
                                Document = doc,
                                Key = metadata.Value<string>("@id"),
                            }).ToArray();

            await DatabaseCommands.BatchAsync(commands);

            Report(String.Format("Wrote {0} documents  in {1:#,#;;0} ms",
                                 batch.Count, sw.ElapsedMilliseconds));
        }

		public override void OnError()
		{

		}
    }
}
