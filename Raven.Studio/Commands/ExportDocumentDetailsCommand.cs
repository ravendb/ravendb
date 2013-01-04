using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Kent.Boogaart.KBCsv;
using Raven.Abstractions.Data;
using Raven.Studio.Controls;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using ColumnDefinition = Raven.Studio.Features.Documents.ColumnDefinition;

namespace Raven.Studio.Commands
{
    public class ExportDocumentDetailsCommand : Command
    {
        private readonly DocumentsModel model;

        public ExportDocumentDetailsCommand(DocumentsModel model)
        {
            this.model = model;
        }

        public override void Execute(object parameter)
        {
            var stream = GetOutputStream();

            if (stream == null)
                return;

            var columns = model.Columns.Columns.ToList();
            var collectionSource = model.Documents.Source;

            if (model.DocumentsHaveId)
            {
                columns.Insert(0, new ColumnDefinition() { Binding = "$JsonDocument:Key", Header = "Id" });
            }

            var cts = new CancellationTokenSource();

            var progressWindow = new ProgressWindow() { Title = "Exporting Report"};
            progressWindow.Closed += delegate { cts.Cancel(); };

            var exporter = new Exporter(stream, collectionSource, columns);

            var exportTask = exporter.ExportAsync(cts.Token, progress => progressWindow.Progress = progress)
                                     .ContinueOnUIThread(t =>
                                     {
                                         // there's a bug in silverlight where if a ChildWindow gets closed too soon after it's opened, it leaves the UI
                                         // disabled; so delay closing the window by a few milliseconds
                                         TaskEx.Delay(TimeSpan.FromMilliseconds(350))
                                               .ContinueOnSuccessInTheUIThread(progressWindow.Close);

                                         if (t.IsFaulted)
                                         {
                                             ApplicationModel.Current.AddErrorNotification(t.Exception,
                                                                                           "Exporting Report Failed");
                                         }
                                         else if (!t.IsCanceled)
                                         {
                                             ApplicationModel.Current.AddInfoNotification("Report Exported Successfully");
                                         }
                                     });

            progressWindow.Show();
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

        private class Exporter
        {
            private readonly Stream stream;
            private readonly IVirtualCollectionSource<ViewableDocument> collectionSource;
            private readonly IList<ColumnDefinition> columns;
            private CsvWriter writer;
            private DocumentColumnsExtractor extractor;
            private const int PageSize = 100;
            private int fetchedDocuments = 0;
            private TaskCompletionSource<bool> tcs;
            private CancellationToken cancellationToken;
            private Action<int> ReportProgress;

            public Exporter(Stream stream, IVirtualCollectionSource<ViewableDocument> collectionSource,
                            IList<ColumnDefinition> columns)
            {
                this.stream = stream;
                this.collectionSource = collectionSource;
                this.columns = columns;
            }

            public Task ExportAsync(CancellationToken cancellationToken, Action<int> reportProgress)
            {
                this.cancellationToken = cancellationToken;
                this.ReportProgress = reportProgress ?? delegate { };

                tcs = new TaskCompletionSource<bool>();
                writer = new CsvWriter(stream);
                extractor = new DocumentColumnsExtractor(columns);

                writer.WriteHeaderRecord(columns.Select(c => c.Header));

                FetchNextPage();

                return tcs.Task;
            }

            private void FetchNextPage()
            {
                collectionSource.GetPageAsync(fetchedDocuments, PageSize, null).ContinueOnUIThread(t =>
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        tcs.SetCanceled();
                        CleanUp();
                        return;
                    }

                    if (!t.IsFaulted)
                    {
                        WriteColumnsForDocuments(writer, t.Result, extractor);
                        fetchedDocuments += t.Result.Count;

                        ReportProgress((int) (((double) fetchedDocuments/collectionSource.Count)*100));

                        if (fetchedDocuments < collectionSource.Count)
                        {
                            FetchNextPage();
                        }
                        else
                        {
                            tcs.SetResult(true);
                            CleanUp();
                        }
                    }
                    else
                    {
                        tcs.SetException(t.Exception);
                        CleanUp();
                    }
                });
            }

            private void CleanUp()
            {
                writer.Close();
                stream.Close();
            }

            private void WriteColumnsForDocuments(CsvWriter writer, IList<ViewableDocument> documents, DocumentColumnsExtractor extractor)
            {
                foreach (var document in documents)
                {
                    var values = extractor.GetValues(document);
                    writer.WriteDataRecord(values);
                }
            }
        }
    }
}
