using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

		public RavenProfilingHandler()
		{
			
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

				context.Response.Output.Write(JS);
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

		const string JS = @"
			if (!window.jQuery) {
				alert('Please add a reference to jQuery to use RavenDB profiling');
			}

			var RavenDBProfiler = (function ($) {
				var options,
					container,
					popupButton,
					resultDialog;

				var load = function () {
					if (options.id.length == 0)
						return;
					$.get('/ravendb/profiling', { id: options.id.join(',') }, function (obj) {
						if (obj)
							addResult(obj);
					}, 'json');
				};

				var addResult = function (resultList) {
					if (!popupButton)
						createUI();

					resultList.forEach(function (result) {
						var resultContainer = $('<div class=""resultContainer""><span>Id: ' + result.Id + '</span></div>')
							.appendTo(container)
						result.Requests.forEach(function (request) {
							$('<div>' + request.Url + '</div>')
							.appendTo(resultContainer);
						});
					});
				};

				var createUI = function () {
					$('<style>.rdbprofilerbutton { position:absolute; left: 0; top: 0; background: PowderBlue; border: 1px solid black; cursor: pointer; border-radius: 2px; padding: 0.1em; } .ravendb-profiler-results { display: none; position:absolute; left: 0; top: 1em; border: 1px solid black; background: white; padding: 2em; border-radius: 5px; }</style>')
						.appendTo('body');
					popupButton = $('<span class=""rdbprofilerbutton"">RavenDb</span>')
						.appendTo('body')
						.click(function () {
							container.toggle();
						});
				};

				return {
					initalize: function (opt) {
						options = opt || {};
						container = $('<div class=""ravendb-profiler-results""></div>')
							.appendTo('body');
						load();
					}
				}
			})(jQuery);";
	}
}