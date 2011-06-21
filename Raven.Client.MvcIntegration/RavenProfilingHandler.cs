using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Routing;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;

namespace Raven.Client.MvcIntegration
{
	public class RavenProfilingHandler : IRouteHandler, IHttpHandler
	{
		private readonly ConcurrentDictionary<DocumentStore, object> stores = new ConcurrentDictionary<DocumentStore, object>();

		static RavenProfilingHandler()
		{
			using(var stream = typeof(RavenProfilingHandler).Assembly.GetManifestResourceStream("Raven.Client.MvcIntegration.ravendb-profiler-scripts.js"))
			{
				ravenDbProfilerScripts = new StreamReader(stream).ReadToEnd();
			}
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
			if (context.Request.AppRelativeCurrentExecutionFilePath == "~/ravendb-profiler-scripts.js")
			{
				context.Response.ContentType = "application/x-javascript";

				context.Response.Output.Write(ravenDbProfilerScripts);
				context.Response.Output.Flush();
			}
			else
			{
				context.Response.ContentType = "application/json";

				var rawIds = context.Request.QueryString.GetValues("id") ?? Enumerable.Empty<string>();
				var ids = rawIds.Select(Guid.Parse);

				var items = from documentStore in stores.Keys
							from id in ids
							let profilingInformation = documentStore.GetProfilingInformationFor(id)
							where profilingInformation != null
							select profilingInformation;

				var results = items.ToList();

				CreateJsonSerializer().Serialize(context.Response.Output, results);

				context.Response.Output.Flush();
			}
		}

		private static JsonSerializer CreateJsonSerializer()
		{
			var jsonSerializer = new JsonSerializer();
			foreach (var jsonConverter in Default.Converters)
			{
				jsonSerializer.Converters.Add(jsonConverter);
			}
			return jsonSerializer;
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
			documentStore.SessionCreatedInternal += operations => RavenProfiler.ContextualSessionList.Add(operations.Id);

			stores.TryAdd(documentStore, null);
		}

		private static readonly string ravenDbProfilerScripts;
	}
}