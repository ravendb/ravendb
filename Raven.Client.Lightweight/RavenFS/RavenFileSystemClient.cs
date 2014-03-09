using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.RavenFS.Changes;
using Raven.Client.RavenFS.Connections;
using Raven.Client.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.RavenFS
{
	public class RavenFileSystemClient : IDisposable, IHoldProfilingInformation
	{
		private readonly ServerNotifications notifications;
        private readonly OperationCredentials credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;
		private IDisposable failedUploadsObserver;
		private readonly RavenFileSystemReplicationInformer replicationInformer;
		private readonly FileConvention convention;
		private int readStripingBase;
		private HttpJsonRequestFactory jsonRequestFactory =
#if !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

		private const int DefaultNumberOfCachedRequests = 2048;


		private readonly ConcurrentDictionary<Guid, CancellationTokenSource> uploadCancellationTokens =
			new ConcurrentDictionary<Guid, CancellationTokenSource>();

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { replicationInformer.FailoverStatusChanged += value; }
			remove { replicationInformer.FailoverStatusChanged -= value; }
		}

		/// <summary>
		/// Allow access to the replication informer used to determine how we replicate requests
		/// </summary>
		public RavenFileSystemReplicationInformer ReplicationInformer
		{
			get { return replicationInformer; }
		}

        public RavenFileSystemClient(string baseUrl, string fileSystemName, OperationCredentials credentials = null)
		{
			ServerUrl = baseUrl;
			if (ServerUrl.EndsWith("/"))
                ServerUrl = ServerUrl.Substring(0, ServerUrl.Length - 1);

            FileSystemName = fileSystemName;

            credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = credentials ?? new OperationCredentials("", new CredentialCache());
			convention = new FileConvention();
			notifications = new ServerNotifications(baseUrl, convention);
			replicationInformer = new RavenFileSystemReplicationInformer(convention);
			readStripingBase = replicationInformer.GetReadStripingBase();
		}

        public string ServerUrl { get; private set; }

        public string FileSystemName { get; private set; }

        public string FileSystemUrl
        {
            get { return string.Format("{0}/ravenfs/{1}", ServerUrl, FileSystemName); }
        }

		public bool IsObservingFailedUploads
		{
			get { return failedUploadsObserver != null; }
			set
			{
				if (value)
				{
					failedUploadsObserver = notifications.FailedUploads().Subscribe(CancelFileUpload);
				}
				else
				{
					failedUploadsObserver.Dispose();
					failedUploadsObserver = null;
				}
			}
		}

		public Task<ServerStats> StatsAsync()
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var requestUriString = operation.Url + "/stats";
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
						"GET", operation.Credentials, convention));

				try
				{
					var response = await request.ReadResponseJsonAsync();

					return new JsonSerializer().Deserialize<ServerStats>(new RavenJTokenReader(response));
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		public Task DeleteAsync(string filename)
		{
			return ExecuteWithReplication("DELETE", async operation =>
			{
				var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename);

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
						"DELETE", operation.Credentials, convention));

				try
				{
					await request.ExecuteRequestAsync();
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		public Task RenameAsync(string filename, string rename)
		{
			return ExecuteWithReplication("PATCH", async operation =>
			{
				var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename) + "?rename=" +
									   Uri.EscapeDataString(rename);

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
						"PATCH", operation.Credentials, convention));

				try
				{
					await request.ExecuteRequestAsync();
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		public Task<FileInfo[]> BrowseAsync(int start = 0, int pageSize = 25)
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,(operation.Url + "/files?start=" + start + "&pageSize=" + pageSize).NoCache() ,
						"GET", operation.Credentials, convention));

				try
				{
					var response = await request.ReadResponseJsonAsync();
					using (var jsonTextReader = new RavenJTokenReader(response))
					{
						return new JsonSerializer
						{
							Converters =
										{
											new NameValueCollectionJsonConverter()
										}
						}.Deserialize<FileInfo[]>(jsonTextReader);
					}
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		private int requestCount;
		private volatile bool currentlyExecuting;

		private Task<T> ExecuteWithReplication<T>(string method, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && convention.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

			currentlyExecuting = true;
			try
			{
				return replicationInformer.ExecuteWithReplicationAsync(method, string.Format("{0}/ravenfs/{1}", ServerUrl, FileSystemName), 
                    credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, currentRequest, readStripingBase, operation)
					.ContinueWith(task =>
					{
						currentlyExecuting = false;
						return task;
					}).Unwrap();
			}
			catch (Exception)
			{
				currentlyExecuting = false;
				throw;
			}
		}

		private Task ExecuteWithReplication(string method, Func<OperationMetadata, Task> operation)
		{
			// Convert the Func<string, Task> to a Func<string, Task<object>>
		    return ExecuteWithReplication(method, async u =>
		    {
		        await operation(u);
		        return (object) null;
		    });
		}

		public Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 25)
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var requestUriString = string.Format("{0}/search/terms?start={1}&pageSize={2}", operation.Url, start, pageSize).NoCache();
				var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
							"GET", operation.Credentials, convention));

				try
				{
					var response = await request.ReadResponseJsonAsync();
					{
						return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
					}
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		public Task<SearchResults> SearchAsync(string query, string[] sortFields = null, int start = 0, int pageSize = 25)
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var requestUriBuilder = new StringBuilder(operation.Url)
					.Append("/search/?query=")
					.Append(Uri.EscapeUriString(query))
					.Append("&start=")
					.Append(start)
					.Append("&pageSize=")
					.Append(pageSize);

				if (sortFields != null)
				{
					foreach (var sortField in sortFields)
					{
						requestUriBuilder.Append("&sort=").Append(sortField);
					}
				}

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString().NoCache(),
						"GET", operation.Credentials, convention));
				try
				{
					var response = await request.ReadResponseJsonAsync();
					using (var jsonTextReader = new RavenJTokenReader(response))
					{
						return new JsonSerializer
						{
							Converters =
										{
											new NameValueCollectionJsonConverter()
										}
						}.Deserialize<SearchResults>(jsonTextReader);
					}
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});

		}

		public Task<NameValueCollection> GetMetadataForAsync(string filename)
		{
			return ExecuteWithReplication("HEAD", async operation =>
			{
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename),
					"HEAD", operation.Credentials, convention));
				try
				{
					await request.ExecuteRequestAsync();
					return request.ResponseHeaders;
				}
				catch (Exception e)
				{
					var aggregateException = e as AggregateException;
					
					var responseException = e as ErrorResponseException;
					if (responseException == null && aggregateException != null)
						responseException = aggregateException.ExtractSingleInnerException() as ErrorResponseException;
					if (responseException != null)
					{
						if (responseException.StatusCode == HttpStatusCode.NotFound)
							return null;	
						throw e.TryThrowBetterError();
					}

					throw e.TryThrowBetterError();
				}
			});
		}

		public Task<NameValueCollection> DownloadAsync(string filename, Stream destination, long? from = null, long? to = null)
		{
			return DownloadAsync("/files/", filename, destination, from, to);
		}

		private Task<NameValueCollection> DownloadAsync(string path, string filename, Stream destination,
															  long? from = null, long? to = null,
															  Action<string, long> progress = null)
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var collection = new NameValueCollection();
				if (destination.CanWrite == false)
					throw new ArgumentException("Stream does not support writing");

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + path + filename,
						"GET", operation.Credentials, convention));

				if (from != null)
				{
					if (to != null)
						request.AddRange(from.Value, to.Value);
					else
						request.AddRange(from.Value);
				}
				else if (destination.CanSeek)
				{
					destination.Position = destination.Length;
					request.AddRange(destination.Position);
				}

				try
				{
					using (var responseStream = new MemoryStream(await request.ReadResponseBytesAsync()))
					{
						foreach (var header in request.ResponseHeaders.AllKeys)
						{
							collection[header] = request.ResponseHeaders[header];
						}
						await responseStream.CopyToAsync(destination, i =>
						{
							if (progress != null)
								progress(filename, i);
						});
					}

					return collection;
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});

		}

		public Task UpdateMetadataAsync(string filename, NameValueCollection metadata)
		{
			return ExecuteWithReplication("POST", async operation =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
						operation.Url + "/files/" + filename,
						"POST", operation.Credentials, convention));

				AddHeaders(metadata, request);

				try
				{
					await request.ExecuteRequestAsync();
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		public Task UploadAsync(string filename, Stream source)
		{
			return UploadAsync(filename, new NameValueCollection(), source, null);
		}

		public Task UploadAsync(string filename, NameValueCollection metadata, Stream source)
		{
			return UploadAsync(filename, metadata, source, null);
		}

		public Task UploadAsync(string filename, NameValueCollection metadata, Stream source,
									  Action<string, long> progress)
		{
			return ExecuteWithReplication("PUT", async operation =>
			{
				if (source.CanRead == false)
					throw new Exception("Stream does not support reading");

				var uploadIdentifier = Guid.NewGuid();
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
						operation.Url + "/files?name=" + Uri.EscapeDataString(filename) + "&uploadId=" + uploadIdentifier,
						"PUT", operation.Credentials, convention));

				metadata.Add("RavenFS-Size", source.Length.ToString());

				AddHeaders(metadata, request);

				var cts = new CancellationTokenSource();

				RegisterUploadOperation(uploadIdentifier, cts);

				try
				{
					await request.WriteAsync(source);
					
					//using (var destination = await request.GetRequestStreamAsync())
					//{
					//	await source.CopyToAsync(destination, written =>
					//	{
					//		if (progress != null)
					//			progress(filename, written);
					//	}, cts.Token);

					//	using (await request.GetResponseAsync())
					//	{
					//	}
					//}
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
				finally
				{
					UnregisterUploadOperation(uploadIdentifier);
				}
			});
		}

		private void CancelFileUpload(UploadFailed uploadFailed)
		{
			CancellationTokenSource cts;
			if (uploadCancellationTokens.TryGetValue(uploadFailed.UploadId, out cts))
			{
				cts.Cancel();
			}
		}

		private void RegisterUploadOperation(Guid uploadId, CancellationTokenSource cts)
		{
			if (IsObservingFailedUploads)
				uploadCancellationTokens.TryAdd(uploadId, cts);
		}

		private void UnregisterUploadOperation(Guid uploadId)
		{
			if (IsObservingFailedUploads)
			{
				CancellationTokenSource cts;
				uploadCancellationTokens.TryRemove(uploadId, out cts);
			}
		}

		public SynchronizationClient Synchronization
		{
			get
			{
				return new SynchronizationClient(this, Convention);
			}
		}

		public ConfigurationClient Config
		{
			get { return new ConfigurationClient(this, Convention); }
		}

		public StorageClient Storage
		{
			get
			{
				return new StorageClient(this, Convention);
			}
		}

        public AdminClient Admin
        {
            get
            {
                return new AdminClient(this, Convention);
            }
        }

		public IServerNotifications Notifications
		{
			get { return notifications; }
		}

		public FileConvention Convention
		{
			get { return convention; }
		}

		private static void AddHeaders(NameValueCollection metadata, HttpJsonRequest request)
		{
			foreach (var key in metadata.AllKeys)
			{
				var values = metadata.GetValues(key);
				if (values == null)
					continue;
				foreach (var value in values)
				{
					request.AddHeader(key, value);
				}
			}
		}

		public Task<string[]> GetFoldersAsync(string from = null, int start = 0, int pageSize = 25)
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var path = @from ?? "";
				if (path.StartsWith("/"))
					path = path.Substring(1);

				var requestUriString = operation.Url + "/folders/subdirectories/" + Uri.EscapeUriString(path) + "?pageSize=" +
									   pageSize + "&start=" + start;

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
						"GET", operation.Credentials, convention));

				try
				{
					var response = await request.ReadResponseJsonAsync();
					
					return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		public Task<SearchResults> GetFilesAsync(string folder, FilesSortOptions options = FilesSortOptions.Default,
												 string fileNameSearchPattern = "", int start = 0, int pageSize = 25)
		{
			var folderQueryPart = GetFolderQueryPart(folder);

			if (string.IsNullOrEmpty(fileNameSearchPattern) == false && fileNameSearchPattern.Contains("*") == false &&
				fileNameSearchPattern.Contains("?") == false)
			{
				fileNameSearchPattern = fileNameSearchPattern + "*";
			}
			var fileNameQueryPart = GetFileNameQueryPart(fileNameSearchPattern);

			return SearchAsync(folderQueryPart + fileNameQueryPart, GetSortFields(options), start, pageSize);
		}

		public Task<Guid> GetServerId()
		{
			return ExecuteWithReplication("GET", async operation =>
			{
				var requestUriString = operation.Url + "/staticfs/id/";

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
						"GET", operation.Credentials, convention));

				try
				{
					var response = await request.ReadResponseJsonAsync();
					return new JsonSerializer().Deserialize<Guid>(new RavenJTokenReader(response));
				}
				catch (Exception e)
				{
					throw e.TryThrowBetterError();
				}
			});
		}

		private static string GetFileNameQueryPart(string fileNameSearchPattern)
		{
			if (string.IsNullOrEmpty(fileNameSearchPattern))
				return "";

			if (fileNameSearchPattern.StartsWith("*") || (fileNameSearchPattern.StartsWith("?")))
				return " AND __rfileName:" + Reverse(fileNameSearchPattern);

			return " AND __fileName:" + fileNameSearchPattern;
		}

		private static string Reverse(string value)
		{
			var characters = value.ToCharArray();
			Array.Reverse(characters);

			return new string(characters);
		}

		private static string GetFolderQueryPart(string folder)
		{
			if (folder == null) throw new ArgumentNullException("folder");
			if (folder.StartsWith("/") == false)
				throw new ArgumentException("folder must starts with a /", "folder");

			int level;
			if (folder == "/")
				level = 1;
			else
				level = folder.Count(ch => ch == '/') + 1;

			var folderQueryPart = "__directory:" + folder + " AND __level:" + level;
			return folderQueryPart;
		}

		private static string[] GetSortFields(FilesSortOptions options)
		{
			string sort = null;
			switch (options & ~FilesSortOptions.Desc)
			{
				case FilesSortOptions.Name:
					sort = "__key";
					break;
				case FilesSortOptions.Size:
					sort = "__size";
					break;
				case FilesSortOptions.LastModified:
					sort = "__modified";
					break;
			}

			if (options.HasFlag(FilesSortOptions.Desc))
			{
				if (string.IsNullOrEmpty(sort))
					throw new ArgumentException("options");
				sort = "-" + sort;
			}

			var sortFields = string.IsNullOrEmpty(sort) ? null : new[] { sort };
			return sortFields;
		}

		public class ConfigurationClient : IHoldProfilingInformation
		{
			private readonly RavenFileSystemClient ravenFileSystemClient;
			private readonly FileConvention convention;
			private readonly JsonSerializer jsonSerializer;
			private HttpJsonRequestFactory jsonRequestFactory =
#if !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

			public ConfigurationClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
			{
				jsonSerializer = new JsonSerializer
				{
					Converters =
						{
							new NameValueCollectionJsonConverter()
						}
				};

				this.ravenFileSystemClient = ravenFileSystemClient;
				this.convention = convention;
			}

			public Task<string[]> GetConfigNames(int start = 0, int pageSize = 25)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = operation.Url + "/config?start=" + start + "&pageSize=" + pageSize;

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						return jsonSerializer.Deserialize<string[]>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task SetConfig(string name, NameValueCollection data)
			{
				return ravenFileSystemClient.ExecuteWithReplication("PUT", async operation =>
				{
					var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(name);
					var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
						"PUT", operation.Credentials, convention));

					using(var ms = new MemoryStream())
					using (var streamWriter = new StreamWriter(ms))
					{
						jsonSerializer.Serialize(streamWriter, data);
						streamWriter.Flush();
						ms.Position = 0;
						await request.WriteAsync(ms);
					}
				});
			}

            public Task SetDestinationsConfig(params SynchronizationDestination[] destinations)
            {
                return ravenFileSystemClient.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(SynchronizationConstants.RavenSynchronizationDestinations);
                    var request =
                    jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "PUT", operation.Credentials, convention));

                    var data = new NameValueCollection();

                    foreach (var synchronizationDestination in destinations)
                    {
                        data.Add("destination", JsonConvert.SerializeObject(synchronizationDestination));
                    }

                    using (var ms = new MemoryStream())
                    using (var streamWriter = new StreamWriter(ms))
                    {
                        jsonSerializer.Serialize(streamWriter, data);
                        streamWriter.Flush();
                        ms.Position = 0;
                        await request.WriteAsync(ms);
                    }
                });
            }

			public Task DeleteConfig(string name)
			{
				return ravenFileSystemClient.ExecuteWithReplication("DELETE", operation =>
				{
					var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(name);

					var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
						"DELETE", operation.Credentials, convention));

					return request.ExecuteRequestAsync();
				});
			}

			public Task<NameValueCollection> GetConfig(string name)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(name);

					var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
						"GET", operation.Credentials, convention));
					
					try
					{
						var response = await request.ReadResponseJsonAsync();
						return jsonSerializer.Deserialize<NameValueCollection>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						var responseException = e as ErrorResponseException;
						if (responseException == null)
							throw;

						if (responseException.StatusCode == HttpStatusCode.NotFound)
							return null;
						throw;
					}
				});
			}

			public Task<ConfigSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriBuilder = new StringBuilder(operation.Url)
						.Append("/config/search/?prefix=")
						.Append(Uri.EscapeUriString(prefix))
						.Append("&start=")
						.Append(start)
						.Append("&pageSize=")
						.Append(pageSize);

					var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString().NoCache(),
						"GET", operation.Credentials, convention));
				
					try
					{
						var response = await request.ReadResponseJsonAsync();
						using (var jsonTextReader = new RavenJTokenReader(response))
						{
							return new JsonSerializer().Deserialize<ConfigSearchResults>(jsonTextReader);
						}
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public ProfilingInformation ProfilingInformation { get; private set; }
		}

		public class SynchronizationClient : IHoldProfilingInformation
		{
			private readonly RavenFileSystemClient ravenFileSystemClient;
			private readonly FileConvention convention;

			private HttpJsonRequestFactory jsonRequestFactory =
#if !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

			public SynchronizationClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
			{
				this.ravenFileSystemClient = ravenFileSystemClient;
				this.convention = convention;
			}

			public Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null)
			{
				return ravenFileSystemClient.DownloadAsync("/rdc/signatures/", sigName, destination, from, to);
			}

			public Task<SignatureManifest> GetRdcManifestAsync(string path)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = operation.Url + "/rdc/manifest/" + StringUtils.UrlEncode(path);
					var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
						"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						return new JsonSerializer().Deserialize<SignatureManifest>(new RavenJTokenReader(response));	
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingAll = false)
			{
				return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/ToDestinations?forceSyncingAll={1}", operation.Url,
														 forceSyncingAll);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"POST", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						return new JsonSerializer().Deserialize<DestinationSyncResult[]>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

            public Task<SynchronizationReport> StartAsync(string fileName, RavenFileSystemClient destination)
            {
                return StartAsync(fileName, destination.ServerUrl, destination.FileSystemName);
            }

			public Task<SynchronizationReport> StartAsync(string fileName, string destinationServerUrl, string destinationFileSystem)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
                    var requestUriString = String.Format("{0}/synchronization/start/{1}?destinationServerUrl={2}&destinationFileSystem={3}", operation.Url,
                                                         Uri.EscapeDataString(fileName), Uri.EscapeDataString(destinationServerUrl), destinationFileSystem);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
							"POST", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<SynchronizationReport> GetSynchronizationStatusAsync(string fileName)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/status/{1}", operation.Url, Uri.EscapeDataString(fileName));

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/resolveConflict/{1}?strategy={2}",
														 operation.Url, Uri.EscapeDataString(filename),
														 Uri.EscapeDataString(strategy.ToString()));

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
							"PATCH", operation.Credentials, convention));

					try
					{
						await request.ExecuteRequestAsync();
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId,
												   IList<HistoryItem> remoteHistory, string remoteServerUrl)
			{
				return ravenFileSystemClient.ExecuteWithReplication("PATCH", async operation =>
				{
					var requestUriString =
						String.Format("{0}/synchronization/applyConflict/{1}?remoteVersion={2}&remoteServerId={3}&remoteServerUrl={4}",
									  operation.Url, Uri.EscapeDataString(filename), remoteVersion,
									  Uri.EscapeDataString(remoteServerId), Uri.EscapeDataString(remoteServerUrl));

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
							"PATCH", operation.Credentials, convention));

					try
					{
						using (var stream = new MemoryStream())
						{
							var sb = new StringBuilder();
							var jw = new JsonTextWriter(new StringWriter(sb));
							new JsonSerializer().Serialize(jw, remoteHistory);
							var bytes = Encoding.UTF8.GetBytes(sb.ToString());

							await stream.WriteAsync(bytes, 0, bytes.Length);
							stream.Position = 0;
							await request.WriteAsync(stream);
						}
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<ListPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/finished?start={1}&pageSize={2}", operation.Url, page,
														 pageSize);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						var preResult =
							new JsonSerializer().Deserialize<ListPage<SynchronizationReport>>(new RavenJTokenReader(response));
						return preResult;
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<ListPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/active?start={1}&pageSize={2}",
														 operation.Url, page, pageSize);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						var preResult =
							new JsonSerializer().Deserialize<ListPage<SynchronizationDetails>>(new RavenJTokenReader(response));
						return preResult;
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<ListPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/pending?start={1}&pageSize={2}",
														 operation.Url, page, pageSize);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();

						var preResult =
							new JsonSerializer().Deserialize<ListPage<SynchronizationDetails>>(new RavenJTokenReader(response));
						return preResult;

					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/LastSynchronization?from={1}",
														 operation.Url, serverId);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						var preResult =
							new JsonSerializer().Deserialize<SourceSynchronizationInformation>(new RavenJTokenReader(response));
						return preResult;
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<IEnumerable<SynchronizationConfirmation>> ConfirmFilesAsync(IEnumerable<Tuple<string, Guid>> sentFiles)
			{
				return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/Confirm", operation.Url);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
							"POST", operation.Credentials, convention));
				
					try
					{
						using (var stream = new MemoryStream())
						{
							var sb = new StringBuilder();
							var jw = new JsonTextWriter(new StringWriter(sb));
							new JsonSerializer().Serialize(jw, sentFiles);
							var bytes = Encoding.UTF8.GetBytes(sb.ToString());

							await stream.WriteAsync(bytes, 0, bytes.Length);
							stream.Position = 0;
							await request.WriteAsync(stream);
							var response = await request.ReadResponseJsonAsync();

							return new JsonSerializer().Deserialize<IEnumerable<SynchronizationConfirmation>>(
								new RavenJTokenReader(response));
						}
						
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<ListPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25)
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = String.Format("{0}/synchronization/conflicts?start={1}&pageSize={2}", operation.Url, page,
														 pageSize);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						var preResult =
							new JsonSerializer().Deserialize<ListPage<ConflictItem>>(new RavenJTokenReader(response));
						return preResult;
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task IncrementLastETagAsync(Guid sourceServerId, string sourceServerUrl, Guid sourceFileETag)
			{
				return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
				{
					var requestUriString =
						String.Format("{0}/synchronization/IncrementLastETag?sourceServerId={1}&sourceServerUrl={2}&sourceFileETag={3}",
									  operation.Url, sourceServerId, sourceServerUrl, sourceFileETag);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"POST", operation.Credentials, convention));

					try
					{
						await request.ExecuteRequestAsync();
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task<RdcStats> GetRdcStatsAsync()
			{
				return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
				{
					var requestUriString = operation.Url + "/rdc/stats";

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"GET", operation.Credentials, convention));

					try
					{
						var response = await request.ReadResponseJsonAsync();
						return new JsonSerializer().Deserialize<RdcStats>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public ProfilingInformation ProfilingInformation { get; private set; }
		}

		public class StorageClient : IHoldProfilingInformation
		{
			private readonly RavenFileSystemClient ravenFileSystemClient;
			private readonly FileConvention convention;
			private HttpJsonRequestFactory jsonRequestFactory =
#if !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

			public StorageClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
			{
				this.ravenFileSystemClient = ravenFileSystemClient;
				this.convention = convention;
			}

			public Task CleanUp()
			{
				return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
				{
					var requestUriString = String.Format("{0}/storage/cleanup", operation.Url);

					var request =
						jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
							"POST", operation.Credentials, convention));

					try
					{
						await request.ExecuteRequestAsync();
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public Task RetryRenaming()
			{
				return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
				{
					var requestUriString = String.Format("{0}/storage/retryrenaming", operation.Url);

					var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
						"POST", operation.Credentials, convention));

					try
					{
						await request.ExecuteRequestAsync();
					}
					catch (Exception e)
					{
						throw e.TryThrowBetterError();
					}
				});
			}

			public ProfilingInformation ProfilingInformation { get; private set; }
		}

        public class AdminClient : IHoldProfilingInformation
        {
            private readonly RavenFileSystemClient ravenFileSystemClient;
            private readonly FileConvention convention;
            private HttpJsonRequestFactory jsonRequestFactory =
#if !SILVERLIGHT && !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

            public AdminClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
            {
                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
            }

            public Task CreateFileSystem(DatabaseDocument databaseDocument)
            {
                var requestUriString = string.Format("{0}/ravenfs/admin/{1}", ravenFileSystemClient.ServerUrl,
                                                     ravenFileSystemClient.FileSystemName);

                var request =
                    jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "PUT", ravenFileSystemClient.credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention));

                try
                {
                    return request.WriteAsync(JsonConvert.SerializeObject(databaseDocument));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

		public void Dispose()
		{
			notifications.Dispose();
		}

		public ProfilingInformation ProfilingInformation { get; private set; }
	}
}