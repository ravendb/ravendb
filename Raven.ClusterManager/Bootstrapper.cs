// -----------------------------------------------------------------------
//  <copyright file="Bootstrapper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Nancy;
using Nancy.Bootstrapper;
using Nancy.Conventions;
using Nancy.Responses;
using Nancy.TinyIoc;
using Raven.Client;
using Raven.Client.Document;
using Raven.ClusterManager.Tasks;

namespace Raven.ClusterManager
{
	public class Bootstrapper : DefaultNancyBootstrapper
	{
		protected override void ConfigureApplicationContainer(TinyIoCContainer container)
		{
			// Disable the AutoRegister since we're not using it
			// base.ConfigureApplicationContainer(container);

			var store = new DocumentStore
			{
				ConnectionStringName = "RavenDB",
			};
			store.Initialize();

			container.Register<IDocumentStore>(store);
		}

		protected override void ConfigureRequestContainer(TinyIoCContainer container, NancyContext context)
		{
			base.ConfigureRequestContainer(container, context);

			var store = container.Resolve<IDocumentStore>();
			var documentSession = store.OpenSession();

			container.Register(documentSession);
		}

		protected override void RequestStartup(TinyIoCContainer container, IPipelines pipelines, NancyContext context)
		{
			base.RequestStartup(container, pipelines, context);

			pipelines.AfterRequest.AddItemToEndOfPipeline(ctx =>
			{
				using (var documentSession = container.Resolve<IDocumentSession>())
				{
					if (ctx.Response.StatusCode != HttpStatusCode.InternalServerError)
					{
						documentSession.SaveChanges();
					}
				}
			});
		}

		protected override NancyInternalConfiguration InternalConfiguration
		{
			get
			{
				// Ignore any assembly that is not this one.
				return NancyInternalConfiguration.Default
												 .WithIgnoredAssembly(assembly => assembly.FullName.StartsWith("Raven.ClusterManager") == false &&
																				  assembly.FullName.StartsWith("Nancy") == false);
			}
		}

		protected override void ConfigureConventions(NancyConventions conventions)
		{
			base.ConfigureConventions(conventions);

			conventions.StaticContentsConventions.Add((ctx, rootPath) =>
			{
				var path = ctx.Request.Url.Path.Substring(1); // Remove the starting '/'.
				if (path == "")
					path = "index.html";

				if (path.StartsWith("api/"))
					return null;

				var resourcePath = @"Raven.ClusterManager.Assets";
				var directoryName = Path.GetDirectoryName(path);
				if (string.IsNullOrEmpty(directoryName) == false)
				{
					resourcePath += "." + directoryName.Replace('\\', '.');
				}
				var fileName = Path.GetFileName(path);

				var assembly = typeof (Bootstrapper).Assembly;
				if (assembly.GetManifestResourceInfo(resourcePath + "." + fileName) == null)
				{
					fileName = "index.html";
				}
				
				return new EmbeddedFileResponse(assembly, resourcePath, fileName);
			});
		}

		protected override void ApplicationStartup(TinyIoCContainer container, IPipelines pipelines)
		{
			var store = container.Resolve<IDocumentStore>();
			var healthMonitorTask = new HealthMonitorTask(store);
		}
	}
}