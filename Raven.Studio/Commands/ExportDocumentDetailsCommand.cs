using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;
using Kent.Boogaart.KBCsv;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
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
                columns.Insert(0, new ColumnDefinition { Binding = "$JsonDocument:Key", Header = "Id" });

            var cts = new CancellationTokenSource();

            var progressWindow = new ProgressWindow { Title = "Exporting Report"};
            progressWindow.Closed += delegate { cts.Cancel(); };

            var exporter = new Exporter(stream, (DocumentsVirtualCollectionSourceBase)collectionSource, columns);

	        exporter.ExportAsync(cts.Token, progress => progressWindow.Progress = progress)
	                .ContinueOnUIThread(t =>
	                {
		                // there's a bug in silverlight where if a ChildWindow gets closed too soon after it's opened, it leaves the UI
		                // disabled; so delay closing the window by a few milliseconds
		                TaskEx.Delay(TimeSpan.FromMilliseconds(350))
		                      .ContinueOnSuccessInTheUIThread(progressWindow.Close);

		                if (t.IsFaulted)
		                {
			                ApplicationModel.Current.AddErrorNotification(t.Exception, "Exporting Report Failed");
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
            private readonly DocumentsVirtualCollectionSourceBase collectionSource;
            private readonly IList<ColumnDefinition> columns;
            private const int BatchSize = 100;

            public Exporter(Stream stream, DocumentsVirtualCollectionSourceBase collectionSource,
                            IList<ColumnDefinition> columns)
            {
                this.stream = stream;
                this.collectionSource = collectionSource;
                this.columns = columns;
            }

            public async Task ExportAsync(CancellationToken cancellationToken, Action<int> reportProgress)
            {
                reportProgress = reportProgress ?? delegate { };
                var context = SynchronizationContext.Current;

                using (stream)
                using (var writer = new CsvWriter(stream))
                {
                    var extractor = new DocumentColumnsExtractor(columns);
                    writer.WriteHeaderRecord(columns.Select(c => c.Header));

                    // we do the streaming of documents on a background thread mainly to escape the
                    // SynchronizationContext: when there's no synchronization context involved the
                    // async methods can resume on any thread instead of having to hop back to the UI thread.
                    // This avoids massive overheads
                    await TaskEx.Run(async () =>
                    {
                       var totalResults = new Reference<long>();

                        using (var documentsStream = await collectionSource.StreamAsync(totalResults))
                        {
                            IList<JsonDocument> documentBatch;
                            var fetchedDocuments = 0;

                            do
                            {
                                documentBatch = await GetNextBatch(documentsStream, cancellationToken);

                                fetchedDocuments += documentBatch.Count;

                                // extracting properties from the documents has to be done on the UI thread
                                // because it might involve using FrameworkElements and Silverlight databinding
                                context.Send(delegate
                                {
                                    WriteColumnsForDocuments(writer, documentBatch, extractor);
                                    reportProgress((int) (((double) fetchedDocuments/totalResults.Value)*100));
                                }, null);

                            } while (documentBatch.Count > 0);
                        }

                    }, cancellationToken);
                }
            }

            private async Task<IList<JsonDocument>> GetNextBatch(IAsyncEnumerator<JsonDocument> documentsStream, CancellationToken token)
            {
                var documents = new List<JsonDocument>();

				while (documents.Count < BatchSize && await documentsStream.MoveNextAsync().ConfigureAwait(false))
                {
                    documents.Add(documentsStream.Current);
                    token.ThrowIfCancellationRequested();
                }

                return documents;
            }

            private void WriteColumnsForDocuments(CsvWriter writer, IEnumerable<JsonDocument> documents, DocumentColumnsExtractor extractor)
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