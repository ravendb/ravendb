using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Raven.Abstractions.Commands;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Kent.Boogaart.KBCsv;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
    public class CsvImportCommand : Command
    {
        const int BatchSize = 512;
        private readonly Action<string> output;
        private TaskModel taskModel;
        private int totalCount;

        public CsvImportCommand(TaskModel taskModel, Action<string> output)
        {
            this.output = output;
            this.taskModel = taskModel;
        }

        public override void Execute(object parameter)
        {
            var openFile = new OpenFileDialog
                               {
                                   Filter = "csv|*.csv"
                               };

            if (openFile.ShowDialog() != true)
                return;

            totalCount = 0;

            taskModel.TaskStatus = TaskStatus.Started;
            output(string.Format("Importing from {0}", openFile.File.Name));

            var sw = Stopwatch.StartNew();

            try
            {
                using (var csvReader = new CsvReader(openFile.File.OpenText()))
                {
                    var batch = new List<RavenJObject>();
                    var header = csvReader.ReadHeaderRecord();
                    var entity = Path.GetFileNameWithoutExtension(openFile.File.Name);

                    foreach (var record in csvReader.DataRecords)
                    {
                        var document = new RavenJObject();
                        var id = Guid.NewGuid().ToString("N");
                        foreach (var column in header.Values)
                        {
                            if (string.Compare("id", column, StringComparison.InvariantCultureIgnoreCase) == 0)
                            {
                                id = record[column];
                            }
                            else
                            {
                                document.Add(string.Format("\"{0}\"", column), new RavenJValue(record[column]));
                            }
                        }

                        var metadata = new RavenJObject { { "Raven-Entity-Name", entity } };
                        document.Add("@metadata", metadata);
                        document.Add("@id", id);

                        batch.Add((RavenJObject)document);

                        if (batch.Count >= BatchSize)
                        {
                            FlushBatch(batch.ToList())
                                .ContinueOnSuccess(() => {});
                            batch.Clear();
                        }
                    }
                }

                output(String.Format("Imported {0:#,#;;0} documents in {1:#,#;;0} ms", totalCount, sw.ElapsedMilliseconds));
                taskModel.TaskStatus = TaskStatus.Ended;
            }
            catch (Exception e)
            {
                taskModel.TaskStatus = TaskStatus.Ended;
                throw e;
            }
        }

        Task FlushBatch(List<RavenJObject> batch)
        {
            totalCount += batch.Count;
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

            output(String.Format("Wrote {0} documents  in {1:#,#;;0} ms",
                                 batch.Count, sw.ElapsedMilliseconds));

            return DatabaseCommands
                .BatchAsync(commands);
        }
    }
}
