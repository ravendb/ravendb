using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Ionic.Zlib;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class ExportDatabaseCommand : Command
	{
		const int BatchSize = 512;

		private readonly Action<string> output;
		private Stream stream;
		private GZipStream gZipStream;
		private StreamWriter streamWriter;
		private JsonTextWriter jsonWriter;
		private TaskModel taskModel;
	    private bool includeAttachments;

		public ExportDatabaseCommand(TaskModel taskModel, Action<string> output)
		{
			this.output = output;
			this.taskModel = taskModel;
		}

		public override void Execute(object parameter)
		{
            TaskCheckBox attachmentUI = taskModel.TaskInputs.FirstOrDefault(x => x.Name == "Include Attachments") as TaskCheckBox;
            includeAttachments = attachmentUI != null && (bool)attachmentUI.Value;

			var saveFile = new SaveFileDialog
			{
				DefaultExt = ".ravendump",
				Filter = "Raven Dumps|*.ravendump;*.raven.dump",
			};

			var name = ApplicationModel.Database.Value.Name;
			var normalizedName = new string(name.Select(ch => Path.GetInvalidPathChars().Contains(ch) ? '_' : ch).ToArray());
			var defaultFileName = string.Format("Dump of {0}, {1}", normalizedName, DateTimeOffset.Now.ToString("dd MMM yyyy HH-mm", CultureInfo.InvariantCulture));
			try
			{
				saveFile.DefaultFileName = defaultFileName;
			}
			catch { }

			if (saveFile.ShowDialog() != true)
				return;

			taskModel.CanExecute.Value = false;

			stream = saveFile.OpenFile();
			gZipStream = new GZipStream(stream, CompressionMode.Compress);
			streamWriter = new StreamWriter(gZipStream);
			jsonWriter = new JsonTextWriter(streamWriter)
			{
				Formatting = Formatting.Indented
			};
			taskModel.TaskStatus = TaskStatus.Started;

			output(String.Format("Exporting to {0}", saveFile.SafeFileName));
			jsonWriter.WriteStartObject();

		    Action finalized = () => 
            {
                jsonWriter.WriteEndObject();
                Infrastructure.Execute.OnTheUI(() => Finish(null));
		    };

		    Action readAttachments = () => ReadAttachments(Guid.Empty, 0, callback: finalized);
		    Action readDocuments = () => ReadDocuments(Guid.Empty, 0, callback: includeAttachments ? readAttachments : finalized);

            try
            {
                ReadIndexes(0, callback: readDocuments);
            }
            catch (Exception ex)
            {
                taskModel.ReportError(ex);
				Infrastructure.Execute.OnTheUI(() => Finish(ex));
            }
		}

		private void ReadIndexes(int totalCount, Action callback)
		{
            if (totalCount == 0)
            {
                output("Begin reading indexes");
                jsonWriter.WritePropertyName("Indexes");
                jsonWriter.WriteStartArray();    
            }
            
			var url = ("/indexes/?start=" + totalCount + "&pageSize=" + BatchSize).NoCache();
			var request = DatabaseCommands.CreateRequest(url, "GET");
		    request.ReadResponseJsonAsync()
		           .ContinueOnSuccess(documents =>
		           {
		               var array = ((RavenJArray) documents);
		               if (array.Length == 0)
		               {
		                   output(String.Format("Done with reading indexes, total: {0}", totalCount));
		                   jsonWriter.WriteEndArray();

		                   callback();
		               }
		               else
		               {
		                   totalCount += array.Length;
		                   output(String.Format("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", array.Length,
		                                        totalCount));
		                   foreach (RavenJToken item in array)
		                   {
		                       item.WriteTo(jsonWriter);
		                   }

		                   ReadIndexes(totalCount, callback);
		               }
		           });
		}

		private void ReadDocuments(Guid lastEtag, int totalCount, Action callback)
		{
            if (totalCount == 0)
            {
                output("Begin reading documents");

                jsonWriter.WritePropertyName("Docs");
                jsonWriter.WriteStartArray();
            }

			var url = ("/docs/?pageSize=" + BatchSize + "&etag=" + lastEtag).NoCache();
			var request = DatabaseCommands.CreateRequest(url, "GET");
		    request.ReadResponseJsonAsync()
		           .ContinueOnSuccess(docs =>
		           {
		               var array = ((RavenJArray) docs);
		               if (array.Length == 0)
		               {
		                   output(String.Format("Done with reading documents, total: {0}", totalCount));
                           jsonWriter.WriteEndArray();

		                   callback();
		               }
		               else
		               {
		                   totalCount += array.Length;
		                   output(String.Format("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", array.Length,
		                                        totalCount));
		                   foreach (RavenJToken item in array)
		                   {
		                       item.WriteTo(jsonWriter);
		                   }
		                   lastEtag = new Guid(array.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));

                           ReadDocuments(lastEtag, totalCount, callback);
		               }
		           });
		}

        private void ReadAttachments(Guid lastEtag, int totalCount, Action callback) {
            if (totalCount == 0)
            {
                output("Begin reading attachments");

                jsonWriter.WritePropertyName("Attachments");
                jsonWriter.WriteStartArray();
            }

            var url = ("/static/?pageSize=" + BatchSize + "&etag=" + lastEtag).NoCache();
            var request = DatabaseCommands.CreateRequest(url, "GET");
            request.ReadResponseJsonAsync()
                   .ContinueOnSuccess(attachments =>
                   {
                       var array = ((RavenJArray) attachments);
                       if (array.Length == 0)
                       {
                           output(String.Format("Done with reading attachments, total: {0}", totalCount));
                           jsonWriter.WriteEndArray();

                           callback();
                       }
                       else
                       {
                           totalCount += array.Length;
                           output(String.Format(
                               "Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", array.Length,
                               totalCount));

                           foreach (var item in array)
                           {
                               output(String.Format("Downloading attachment: {0}", item.Value<string>("Key")));

                               var requestData = DatabaseCommands.CreateRequest("/static/" + item.Value<string>("Key"),
                                                                                "GET");
                               requestData.ReadResponseBytesAsync()
                                          .ContinueOnSuccess(attachmentData =>
                                          {
                                              new RavenJObject
                                              {
                                                  {"Data", attachmentData},
                                                  {"Metadata", item.Value<RavenJObject>("Metadata")},
                                                  {"Key", item.Value<string>("Key")}
                                              }.WriteTo(jsonWriter);
                                          });
                           }
                       }

                       lastEtag = new Guid(array.Last().Value<string>("Etag"));
                       ReadAttachments(lastEtag, totalCount, callback);
                   });
        }

		private void Finish(Exception exception)
		{
			streamWriter.Flush();
			streamWriter.Dispose();
			stream.Dispose();

			output("Export complete");
			taskModel.CanExecute.Value = true;
			taskModel.TaskStatus = TaskStatus.Ended;
			if (exception != null)
				output(exception.ToString());
		}
	}
}