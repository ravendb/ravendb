// -----------------------------------------------------------------------
//  <copyright file="Bootstrapper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Nancy;
using Nancy.Conventions;
using Nancy.Responses;

namespace Raven.ClusterManager
{
	public class Bootstrapper : DefaultNancyBootstrapper
	{
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
				return new EmbeddedFileResponse(typeof (Bootstrapper).Assembly, resourcePath, fileName);
			});
		}
	}
}