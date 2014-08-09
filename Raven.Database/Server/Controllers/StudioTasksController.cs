using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Microsoft.VisualBasic.FileIO;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Util;
using Raven.Database.Actions;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Smuggler;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class StudioTasksController : RavenDbApiController
	{
        const int csvImportBatchSize = 512;

		[HttpPost]
		[Route("studio-tasks/import")]
		[Route("databases/{databaseName}/studio-tasks/import")]
		public async Task<HttpResponseMessage> ImportDatabase(int batchSize, bool includeExpiredDocuments, ItemType operateOnTypes, string filtersPipeDelimited, string transformScript)
		{
			if (!Request.Content.IsMimeMultipartContent())
			{
				throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);
			}

			string tempPath = Path.GetTempPath();
			var fullTempPath = tempPath + Constants.TempUploadsDirectoryName;
			if (File.Exists(fullTempPath))
				File.Delete(fullTempPath);
			if (Directory.Exists(fullTempPath) == false)
				Directory.CreateDirectory(fullTempPath);

			var streamProvider = new MultipartFileStreamProvider(fullTempPath);
			await Request.Content.ReadAsMultipartAsync(streamProvider);
			var uploadedFilePath = streamProvider.FileData[0].LocalFileName;
			
			string fileName = null;
			var fileContent = streamProvider.Contents.SingleOrDefault();
			if (fileContent != null)
			{
				fileName = fileContent.Headers.ContentDisposition.FileName.Replace("\"", string.Empty);
			}

			var status = new ImportOperationStatus();
			var cts = new CancellationTokenSource();
			
			var task = Task.Run(async () =>
			{
				try
				{
					using (var fileStream = File.Open(uploadedFilePath, FileMode.Open, FileAccess.Read))
					{
						var dataDumper = new DataDumper(Database);
						dataDumper.Progress += s => status.LastProgress = s;
						var smugglerOptions = dataDumper.SmugglerOptions;
						smugglerOptions.BatchSize = batchSize;
						smugglerOptions.ShouldExcludeExpired = !includeExpiredDocuments;
						smugglerOptions.OperateOnTypes = operateOnTypes;
						smugglerOptions.TransformScript = transformScript;
						smugglerOptions.CancelToken = cts;

						// Filters are passed in without the aid of the model binder. Instead, we pass in a list of FilterSettings using a string like this: pathHere;;;valueHere;;;true|||againPathHere;;;anotherValue;;;false
						// Why? Because I don't see a way to pass a list of a values to a WebAPI method that accepts a file upload, outside of passing in a simple string value and parsing it ourselves.
						if (filtersPipeDelimited != null)
						{
							smugglerOptions.Filters.AddRange(filtersPipeDelimited
								.Split(new string[] { "|||" }, StringSplitOptions.RemoveEmptyEntries)
								.Select(f => f.Split(new string[] { ";;;" }, StringSplitOptions.RemoveEmptyEntries))
								.Select(o => new FilterSetting { Path = o[0], Values = new List<string> { o[1] }, ShouldMatch = bool.Parse(o[2]) }));
						}

						await dataDumper.ImportData(new SmugglerImportOptions { FromStream = fileStream });
					}
				}
				catch (Exception e)
				{
					status.ExceptionDetails = e.ToString();
					if (cts.Token.IsCancellationRequested)
					{
						status.ExceptionDetails = "Task was cancelled";
						cts.Token.ThrowIfCancellationRequested(); //needed for displaying the task status as canceled and not faulted
					}
					throw;
				}
				finally
				{
					status.Completed = true;
					File.Delete(uploadedFilePath);
				}
			}, cts.Token);

			long id;
			Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
			{
				StartTime = SystemTime.UtcNow,
				TaskType = TaskActions.PendingTaskType.ImportDatabase,
				Payload = fileName,
				
			}, out id, cts);

			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

	    public class ExportData
	    {
            public string SmugglerOptions { get; set; }
	    }
        
		[HttpPost]
		[Route("studio-tasks/exportDatabase")]
		[Route("databases/{databaseName}/studio-tasks/exportDatabase")]
        public Task<HttpResponseMessage> ExportDatabase(ExportData smugglerOptionsJson)
		{
            var requestString = smugglerOptionsJson.SmugglerOptions;
	        SmugglerOptions smugglerOptions;
      
            using (var jsonReader = new RavenJsonTextReader(new StringReader(requestString)))
			{
				var serializer = JsonExtensions.CreateDefaultJsonSerializer();
                smugglerOptions = (SmugglerOptions)serializer.Deserialize(jsonReader, typeof(SmugglerOptions));
			}


            var result = GetEmptyMessage();
            
            // create PushStreamContent object that will be called when the output stream will be ready.
			result.Content = new PushStreamContent(async (outputStream, content, arg3) =>
			{
			    try
			    {
				    var dataDumper = new DataDumper(Database, smugglerOptions);
				    await dataDumper.ExportData(
					    new SmugglerExportOptions
					    {
						    ToStream = outputStream
					    }).ConfigureAwait(false);
			    }
			    finally
			    {
			        outputStream.Close();
			    }

				
			});

            result.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
            {
                FileName = string.Format("Dump of {0}, {1}.ravendump", this.DatabaseName, DateTime.Now.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture))
            };
			
			return new CompletedTask<HttpResponseMessage>(result);
		}
        
		[HttpPost]
		[Route("studio-tasks/createSampleData")]
		[Route("databases/{databaseName}/studio-tasks/createSampleData")]
		public async Task<HttpResponseMessage> CreateSampleData()
		{
			var results = Database.Queries.Query(Constants.DocumentsByEntityNameIndex, new IndexQuery(), CancellationToken.None);
			if (results.Results.Count > 0)
			{
				return GetMessageWithString("You cannot create sample data in a database that already contains documents", HttpStatusCode.BadRequest);
			}

			using (var sampleData = typeof(StudioTasksController).Assembly.GetManifestResourceStream("Raven.Database.Server.Assets.EmbeddedData.Northwind.dump"))
			{
				var dataDumper = new DataDumper(Database) {SmugglerOptions = {OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers, ShouldExcludeExpired = false}};
				await dataDumper.ImportData(new SmugglerImportOptions {FromStream = sampleData});
			}

			return GetEmptyMessage();
		}

        [HttpGet]
        [Route("studio-tasks/simulate-sql-replication")]
        [Route("databases/{databaseName}/studio-tasks/simulate-sql-replication")]
        public Task<HttpResponseMessage> SimulateSqlReplication(string documentId, bool performRolledBackTransaction)
        {

            var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
            if (task == null)
				return GetMessageWithObjectAsTask(new
				{
					Error = "SQL Replication bundle is not installed"
				}, HttpStatusCode.NotFound);
            try
            {
                Alert alert = null;
                var sqlReplication =
                    JsonConvert.DeserializeObject<SqlReplicationConfig>(GetQueryStringValue("sqlReplication"));

                // string strDocumentId, SqlReplicationConfig sqlReplication, bool performRolledbackTransaction, out Alert alert, out Dictionary<string,object> parameters
                var results = task.SimulateSqlReplicationSQLQueries(documentId, sqlReplication, performRolledBackTransaction, out alert);

                return GetMessageWithObjectAsTask(new {
                    Results = results,
                    LastAlert = alert
                });
            }
            catch (Exception ex)
            {
                    return GetMessageWithObjectAsTask(new
                    {
                        Error = "Executeion failed",
                        Exception = ex
                    }, HttpStatusCode.BadRequest);
            }
        }

        [HttpGet]
        [Route("studio-tasks/test-sql-replication-connection")]
        [Route("databases/{databaseName}/studio-tasks/test-sql-replication-connection")]
        public Task<HttpResponseMessage> TestSqlReplicationConnection(string factoryName, string connectionString)
        {
            try
            {
                RelationalDatabaseWriter.TestConnection(factoryName, connectionString);
                return GetEmptyMessageAsTask(HttpStatusCode.NoContent);
            }
            catch (Exception ex)
            {
                return GetMessageWithObjectAsTask(new
                {
                    Error = "Connection failed",
                    Exception = ex
                }, HttpStatusCode.BadRequest);
            }
        }

        [HttpGet]
        [Route("studio-tasks/createSampleDataClass")]
        [Route("databases/{databaseName}/studio-tasks/createSampleDataClass")]
        public Task<HttpResponseMessage> CreateSampleDataClass()
        {
            using (var sampleData = typeof(StudioTasksController).Assembly.GetManifestResourceStream("Raven.Database.Server.Assets.EmbeddedData.NorthwindHelpData.cs"))
            {
                if (sampleData == null)
                    return GetEmptyMessageAsTask();
                   
                sampleData.Position = 0;
                using (var reader = new StreamReader(sampleData, Encoding.UTF8))
                {
                   var data = reader.ReadToEnd();
                   return GetMessageWithObjectAsTask(data);
                }
            }
        }

        [HttpGet]
        [Route("studio-tasks/new-encryption-key")]
        public HttpResponseMessage GetNewEncryption(string path = null)
        {
            RandomNumberGenerator randomNumberGenerator = new RNGCryptoServiceProvider();
            var byteStruct = new byte[Constants.DefaultGeneratedEncryptionKeyLength];
            randomNumberGenerator.GetBytes(byteStruct);
            var result = Convert.ToBase64String(byteStruct);

            HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK, result);
            return response;
        }

        [HttpGet]
        [Route("studio-tasks/get-sql-replication-stats")]
        [Route("databases/{databaseName}/studio-tasks/get-sql-replication-stats")]
        public HttpResponseMessage GetSQLReplicationStats(string sqlReplicationName)
        {
            var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
            if (task == null)
                return GetMessageWithObject(new
                {
                    Error = "SQL Replication bundle is not installed"
                }, HttpStatusCode.NotFound);

            var matchingStats = task.Statistics.FirstOrDefault(x => x.Key == sqlReplicationName);

            if (matchingStats.Key != null)
            {
                return GetMessageWithObject(task.Statistics.FirstOrDefault(x => x.Key == sqlReplicationName));
            }
            return GetEmptyMessage(HttpStatusCode.NotFound);
        }

        [HttpPost]
        [Route("studio-tasks/reset-sql-replication")]
        [Route("databases/{databaseName}/studio-tasks/reset-sql-replication")]
        public Task<HttpResponseMessage> ResetSqlReplication(string sqlReplicationName)
        {
            var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
            if (task == null)
                return GetMessageWithObjectAsTask(new
                {
                    Error = "SQL Replication bundle is not installed"
                }, HttpStatusCode.NotFound);
            SqlReplicationStatistics stats;
            task.Statistics.TryRemove(sqlReplicationName, out stats);
            var jsonDocument = Database.Documents.Get(SqlReplicationTask.RavenSqlreplicationStatus, null);
            if (jsonDocument != null)
            {
                var replicationStatus = jsonDocument.DataAsJson.JsonDeserialization<SqlReplicationStatus>();
                replicationStatus.LastReplicatedEtags.RemoveAll(x => x.Name == sqlReplicationName);
                
                Database.Documents.Put(SqlReplicationTask.RavenSqlreplicationStatus, null, RavenJObject.FromObject(replicationStatus), new RavenJObject(), null);
            }

            return GetEmptyMessageAsTask(HttpStatusCode.NoContent);
        }

        [HttpPost]
        [Route("studio-tasks/is-base-64-key")]
        public async Task<HttpResponseMessage> IsBase64Key(string path = null)
        {
            string message = null;
            try
            {
                //Request is of type HttpRequestMessage
                string keyObjectString = await Request.Content.ReadAsStringAsync();
                NameValueCollection nvc = HttpUtility.ParseQueryString(keyObjectString);
                var key = nvc["key"];

                //Convert base64-encoded hash value into a byte array.
                //ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                Convert.FromBase64String(key);
            }
            catch (Exception)
            {
				message = "The key must be in Base64 encoding format!";
            }

			HttpResponseMessage response = Request.CreateResponse((message == null) ? HttpStatusCode.OK : HttpStatusCode.BadRequest, message);
            return response;
        }

        private Task FlushBatch(IEnumerable<RavenJObject> batch)
        {
            var commands = (from doc in batch
                            let metadata = doc.Value<RavenJObject>("@metadata")
                            let removal = doc.Remove("@metadata")
                            select new PutCommandData
                            {
                                Metadata = metadata,
                                Document = doc,
                                Key = metadata.Value<string>("@id"),
                            }).ToArray();

            Database.Batch(commands, CancellationToken.None);
	        return new CompletedTask();
        }

        [HttpGet]
        [Route("studio-tasks/resolveMerge")]
        [Route("databases/{databaseName}/studio-tasks/resolveMerge")]
        public Task<HttpResponseMessage> ResolveMerge(string documentId)
        {
            int nextPage = 0;
            var docs = Database.Documents.GetDocumentsWithIdStartingWith(documentId + "/conflicts", null, null, 0, 1024, CancellationToken.None, ref nextPage);
            var conflictsResolver = new ConflictsResolver(docs.Values<RavenJObject>());
            return GetMessageWithObjectAsTask(conflictsResolver.Resolve());
        }

	    [HttpPost]
        [Route("studio-tasks/loadCsvFile")]
	    [Route("databases/{databaseName}/studio-tasks/loadCsvFile")]
	    public async Task<HttpResponseMessage> LoadCsvFile()
	    {

            if (!Request.Content.IsMimeMultipartContent())
                throw new Exception(); // divided by zero

            var provider = new MultipartMemoryStreamProvider();
            await Request.Content.ReadAsMultipartAsync(provider);

            foreach (var file in provider.Contents)
            {
                var filename = file.Headers.ContentDisposition.FileName.Trim('\"');

                var stream = await file.ReadAsStreamAsync();

                using (var csvReader = new TextFieldParser(stream))
                {
	                csvReader.SetDelimiters(",");
                    var headers = csvReader.ReadFields();
                    var entity =
                        Inflector.Pluralize(CSharpClassName.ConvertToValidClassName(Path.GetFileNameWithoutExtension(filename)));
                    if (entity.Length > 0 && char.IsLower(entity[0]))
                        entity = char.ToUpper(entity[0]) + entity.Substring(1);

                    var totalCount = 0;
                    var batch = new List<RavenJObject>();
                    var columns = headers.Where(x => x.StartsWith("@") == false).ToArray();

                    batch.Clear();
	                while (csvReader.EndOfData == false)
	                {
		                var record = csvReader.ReadFields();
                        var document = new RavenJObject();
                        string id = null;
                        RavenJObject metadata = null;
		                for (int index = 0; index < columns.Length; index++)
		                {
			                var column = columns[index];
			                if (string.IsNullOrEmpty(column))
				                continue;

			                if (string.Equals("id", column, StringComparison.OrdinalIgnoreCase))
			                {
								id = record[index];
			                }
			                else if (string.Equals(Constants.RavenEntityName, column, StringComparison.OrdinalIgnoreCase))
			                {
				                metadata = metadata ?? new RavenJObject();
								metadata[Constants.RavenEntityName] = record[index];
								id = id ?? record[index] + "/";
			                }
			                else if (string.Equals(Constants.RavenClrType, column, StringComparison.OrdinalIgnoreCase))
			                {
				                metadata = metadata ?? new RavenJObject();
								metadata[Constants.RavenClrType] = record[index];
								id = id ?? record[index] + "/";
			                }
			                else
			                {
								document[column] = SetValueInDocument(record[index]);
			                }
		                }

		                metadata = metadata ?? new RavenJObject { { "Raven-Entity-Name", entity } };
                        document.Add("@metadata", metadata);
                        metadata.Add("@id", id ?? Guid.NewGuid().ToString());

                        batch.Add(document);
                        totalCount++;

                        if (batch.Count >= csvImportBatchSize)
                        {
                            await FlushBatch(batch);
                            batch.Clear();
                        }
                    }

                    if (batch.Count > 0)
                    {
                        await FlushBatch(batch);
                    }
                }

            }

            return GetEmptyMessage();
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

		private class ImportOperationStatus
		{
			public bool Completed { get; set; }
			public string LastProgress { get; set; }
			public string ExceptionDetails { get; set; }
		}
	}
}

