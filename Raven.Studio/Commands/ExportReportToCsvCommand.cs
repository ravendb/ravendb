using System.IO;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using Kent.Boogaart.KBCsv;
using Raven.Studio.Controls;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
    public class ExportReportToCsvCommand : Command
    {
        private readonly ReportingModel model;

        public ExportReportToCsvCommand(ReportingModel model)
        {
            this.model = model;
        }

        public override void Execute(object parameter)
        {
            var stream = GetOutputStream();

            if (stream == null)
                return;

             using (stream)
             using (var writer = new CsvWriter(stream))
             {
                 writer.WriteHeaderRecord(new[] {"Key"}.Concat(model.ValueCalculations.Select(v => v.Header)));

                 foreach (var reportRow in model.Results)
                 {
                     writer.WriteDataRecord(new object[] { reportRow.Key}.Concat(model.ValueCalculations.Select(v => (object)reportRow.Values[v.Header])));
                 }
             }
        }

        private Stream GetOutputStream()
        {
            var saveFile = new SaveFileDialog
            {
                DefaultFileName = "Export.csv",
                DefaultExt = ".csv",
                Filter = "CSV (*.csv)|*.csv",
            };

            if (saveFile.ShowDialog() != true)
                return null;

            try
            {
                var stream = saveFile.OpenFile();
                return stream;
            }
            catch (IOException exception)
            {
                ApplicationModel.Current.AddErrorNotification(exception, "Could not open file " + saveFile.SafeFileName);
                return null;
            }
        }
    }
}