using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Microsoft.VisualBasic.FileIO;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Util;
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
		public async Task<HttpResponseMessage> ImportDatabase()
		{
			var dataDumper = new DataDumper(Database);
			var importData = dataDumper.ImportData(new SmugglerImportOptions
			{
				FromStream = await InnerRequest.Content.ReadAsStreamAsync()
			}, new SmugglerOptions());
			throw new InvalidOperationException();
		}


		[HttpPost]
		[Route("studio-tasks/exportDatabase")]
		[Route("databases/{databaseName}/studio-tasks/exportDatabase")]
		public async Task<HttpResponseMessage> ExportDatabase(SmugglerOptionsDto dto)
		{
			var smugglerOptions = new SmugglerOptions();
			// smugglerOptions.OperateOnTypes = ;

			var result = GetEmptyMessage();
			result.Content = new PushStreamContent(async (outputStream, content, arg3) =>
			{
				{
					
				};
				await new DataDumper(Database).ExportData(new SmugglerExportOptions
				{
					ToStream = outputStream
				}, smugglerOptions);
			});
			
			return result;
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
				var smugglerOptions = new SmugglerOptions
				{
					OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers,
					ShouldExcludeExpired = false,
				};
				var dataDumper = new DataDumper(Database);
				await dataDumper.ImportData(new SmugglerImportOptions {FromStream = sampleData}, smugglerOptions);
			}

			return GetEmptyMessage();
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

        [HttpPost]
        [Route("studio-tasks/is-base-64-key")]
        public async Task<HttpResponseMessage> IsBase64Key(string path = null)
        {
            bool result = true;
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
            catch (Exception e)
            {
                result = false;
            }

            HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK, result);
            return response;
        }

        async Task FlushBatch(IEnumerable<RavenJObject> batch)
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

            Database.Batch(commands);
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
	}

	public class SmugglerOptionsDto
	{
		public bool IncludeDocuments { get; set; }
		public bool IncludeIndexes { get; set; }
		public bool IncludeTransformers { get; set; }
		public bool IncludeAttachments { get; set; }
		public bool RemoveAnalyzers { get; set; }
	}
}