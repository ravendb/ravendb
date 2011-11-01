using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Markup;
using System.Windows.Navigation;

namespace Raven.Studio.Infrastructure
{
	[ContentProperty("UriMappings")]
	public class StudioUriMapper : UriMapperBase
	{
		public Collection<StudioUriMapping> UriMappings { get; private set; }

		public StudioUriMapper()
		{
			UriMappings = new Collection<StudioUriMapping>();
		}

		public override Uri MapUri(Uri uri)
		{
			foreach (var uriMapping in UriMappings)
			{
				var mappedUri = uriMapping.MapUri(uri);
				if (mappedUri != null)
					return mappedUri;
			}
			return uri;
		}
	}

	public class StudioUriMapping
	{
		private Uri DefaultUri = new Uri("/Views/Home.xaml", UriKind.Relative);
		const string DatabaseTag = "{_database}";

		public string Uri { get; set; }
		public string MappedUri { get; set; }

		public Uri MapUri(Uri uri)
		{
			var patterns = Uri.Split(new[] {'/'}, StringSplitOptions.RemoveEmptyEntries);
			var values = MappedUri.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
			var url = uri.ToString().Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
			
			for (int i = 0; i < patterns.Length; i++)
			{
				if (patterns[i] == DatabaseTag)
					continue;
				if (url.Length <= i)
					return DefaultUri;
				return new Uri(string.Join("/", MappedUri.Replace("{view}", url[i])), UriKind.Relative);
			}
			return DefaultUri;
		}

		//private string ResolveDatabaseTag(string url)
		//{
		//    var start = Uri.IndexOf(DatabaseTag);
		//    if (start == -1)
		//        return url;
		//    var length = DatabaseTag.Length;
		//    if (start != 0 && Uri[start - 1] == '/')
		//    {
		//        start -= 1;
		//        length += 1;
		//    }

		//    url = url.Remove(start, length);
		//    var database = UrlUtil.GetDatabaseFromUrl(url);
		//    if (database != null)
		//    {
		//        return url.Remove()
				
		//    }
		//}
	}
}