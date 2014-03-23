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
using Kent.Boogaart.KBCsv;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Util;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Newtonsoft.Json;

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
		[Route("studio-tasks/createSampleData")]
		[Route("databases/{databaseName}/studio-tasks/createSampleData")]
		public async Task<HttpResponseMessage> CreateSampleData()
		{
			var results = Database.Query(Constants.DocumentsByEntityNameIndex, new IndexQuery(), CancellationToken.None);
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

                using (var csvReader = new CsvReader(stream))
                {
                    var header = csvReader.ReadHeaderRecord();
                    var entity =
                        Inflector.Pluralize(CSharpClassName.ConvertToValidClassName(Path.GetFileNameWithoutExtension(filename)));
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
}