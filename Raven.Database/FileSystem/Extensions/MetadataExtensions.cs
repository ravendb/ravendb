//-----------------------------------------------------------------------
// <copyright file="MetadataExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;

namespace Raven.Database.FileSystem.Extensions
{

	/// <summary>
	/// Extensions for handling metadata
	/// </summary>
	public static class MetadataExtensions
	{
        public static RavenJObject DropRenameMarkers(this RavenJObject metadata)
		{
			metadata.Remove(SynchronizationConstants.RavenDeleteMarker);
			metadata.Remove(SynchronizationConstants.RavenRenameFile);

			return metadata;
		}

        public static RavenJObject WithRenameMarkers(this RavenJObject metadata, string rename)
		{
			metadata[SynchronizationConstants.RavenDeleteMarker] = "true";
			metadata[SynchronizationConstants.RavenRenameFile] = rename;

			return metadata;
		}

        public static RavenJObject WithDeleteMarker(this RavenJObject metadata)
		{
			metadata[SynchronizationConstants.RavenDeleteMarker] = "true";

			return metadata;
		}

        public static T ValueOrDefault<T>(this IEnumerable<KeyValuePair<string, IEnumerable<string>>> self, string name, T @default)
        {
            try
            {
                return self.Value<T>(name);
            }
            catch
            {
                return @default;
            }
        }

        public static T Value<T>(this IEnumerable<KeyValuePair<string, IEnumerable<string>>> self, string name)
        {
            string value = self.FirstOrDefault(x => x.Key.Equals(name)).Value.FirstOrDefault();
            
			return new JsonSerializer().Deserialize<T>(new JsonTextReader(new StringReader(value)));
		}

        public static void AddHeaders(this HttpWebRequest request, RavenJObject metadata)
        {
            foreach (var item in metadata)
            {
                request.Headers[item.Key] = item.Value.ToString();
            }
        }
	}
}