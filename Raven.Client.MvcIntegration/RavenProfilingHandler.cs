using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Routing;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Client.Document;

namespace Raven.Client.MvcIntegration
{
	public class RavenProfilingHandler : IRouteHandler, IHttpHandler
	{
		private readonly JsonFormatterAndFieldsFilterer jsonFormatterAndFieldsFilterer;
		private readonly ConcurrentDictionary<DocumentStore, object> stores = new ConcurrentDictionary<DocumentStore, object>();
		public static string SourcePath;

		private ConcurrentDictionary<string, string> cache = new ConcurrentDictionary<string, string>();
	
		public RavenProfilingHandler(HashSet<string> fieldsToFilter)
		{
			jsonFormatterAndFieldsFilterer = new JsonFormatterAndFieldsFilterer(fieldsToFilter);
		}

		/// <summary>
		/// Provides the object that processes the request.
		/// </summary>
		/// <returns>
		/// An object that processes the request.
		/// </returns>
		/// <param name="requestContext">An object that encapsulates information about the request.</param>
		public IHttpHandler GetHttpHandler(RequestContext requestContext)
		{
			return this;
		}

		/// <summary>
		/// Enables processing of HTTP Web requests by a custom HttpHandler that implements the <see cref="T:System.Web.IHttpHandler"/> interface.
		/// </summary>
		/// <param name="context">An <see cref="T:System.Web.HttpContext"/> object that provides references to the intrinsic server objects (for example, Request, Response, Session, and Server) used to service HTTP requests. </param>
		public void ProcessRequest(HttpContext context)
		{
			var path = context.Request.QueryString["path"];
			if(string.IsNullOrEmpty(path) == false)
			{
				HandlePathRequest(context, path);
			}
			else
			{
				HandleDataReuqest(context);
			}
		}

		private void HandleDataReuqest(HttpContext context)
		{
			context.Response.ContentType = "application/json";

			var rawIds = context.Request.QueryString.GetValues("id") ?? 
			             context.Request.QueryString.GetValues("id[]") ??
			             Enumerable.Empty<string>();
			var ids = rawIds.Select(Guid.Parse);

			var items = from documentStore in stores.Keys
			            from id in ids
			            let profilingInformation = documentStore.GetProfilingInformationFor(id)
			            where profilingInformation != null
			            select jsonFormatterAndFieldsFilterer.Filter(profilingInformation);

			var results = items.ToList();

			JsonExtensions.CreateDefaultJsonSerializer().Serialize(context.Response.Output, results);

			context.Response.Output.Flush();
		}

		private void HandlePathRequest(HttpContext context, string path)
		{
			if(path.EndsWith(".js"))
				context.Response.ContentType = "application/javascript";
			else if (path.EndsWith(".tmpl.html"))
				context.Response.ContentType = "text/html";
			else if (path.EndsWith(".css"))
				context.Response.ContentType = "text/css";

			if (string.IsNullOrEmpty(SourcePath))
			{
				var value = cache.GetOrAdd(path, s =>
				{
					using (var stream = typeof(RavenProfilingHandler).Assembly.GetManifestResourceStream("Raven.Client.MvcIntegration.Content." + path.Replace("/", ".")))
					{
						return new StreamReader(stream).ReadToEnd();
					}
				});
				context.Response.Output.Write(value);
			}
			else  // debug mode, probably
			{
				var file = Path.Combine(SourcePath, path);
				context.Response.Output.Write(File.ReadAllText(file));
			}
			context.Response.Output.Flush();
		}

		/// <summary>
		/// Gets a value indicating whether another request can use the <see cref="T:System.Web.IHttpHandler"/> instance.
		/// </summary>
		/// <returns>
		/// true if the <see cref="T:System.Web.IHttpHandler"/> instance is reusable; otherwise, false.
		/// </returns>
		public bool IsReusable
		{
			get { return true; }
		}

		public void AddStore(IDocumentStore store)
		{
			var documentStore = store as DocumentStore;
			if (documentStore == null)
				return;

			if (documentStore.WasDisposed)
				return;

			object _;
			documentStore.AfterDispose += (sender, args) => stores.TryRemove(documentStore, out _);
			documentStore.SessionCreatedInternal += OnSessionCreated;

			stores.TryAdd(documentStore, null);
		}

		private void OnSessionCreated(InMemoryDocumentSessionOperations operations)
		{
			RavenProfiler.ContextualSessionList.Add(operations.Id);
			if (HttpContext.Current == null)
				return;
			
			try
			{
				HttpContext.Current.Response.AddHeader("X-RavenDb-Profiling-Id", operations.Id.ToString());
			}
			catch (HttpException)
			{
				// headers were already written, nothing much that we can do here, ignoring
			}
		}
	}
}