using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Client.Silverlight.MissingFromSilverlight
{
	public class NameValueCollection : IEnumerable
	{
		private Dictionary<string, List<string>> inner =
			new Dictionary<string, List<string>>(StringComparer.InvariantCultureIgnoreCase);

		public NameValueCollection()
		{
			inner = new Dictionary<string, List<string>>(StringComparer.InvariantCultureIgnoreCase);
		}

		public Dictionary<string, List<string>> Headers { get { return inner; } }

		public IEnumerator GetEnumerator()
		{
			return inner.Keys.GetEnumerator();
		}

		public string[] GetValues(string header)
		{
			var result = new string[inner.Count];
			var counter = 0;
			foreach (var list in inner)
			{
				result[counter] = string.Join(";", list);
				counter++;
			}

			return result;
		}

		public string this[string key]
		{
			get { return inner[key].FirstOrDefault(); }
			set
			{ 
				if(inner.ContainsKey(key))
					inner[key].Add(value);
				else
					inner.Add(key, new List<string>{value});
			}
		}

		public void Remove(string key)
		{
			inner.Remove(key);
		}

		public bool ContainsKey(string key)
		{
			return inner.ContainsKey(key);
		}
	}
}