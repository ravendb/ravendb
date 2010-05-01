using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;
using System.Linq;

namespace Raven.Database.Data
{
	public class PatchCommandData : ICommandData
	{
		public PatchRequest[] Patches{ get; set;}

		public string Key
		{
			get; set;
		}

		public string Method
		{
			get { return "PATCH"; }
		}

		public Guid? Etag
		{
			get; set;
		}
#if !CLIENT
		public TransactionInformation TransactionInformation
		{
			get; set;
		}

		public void Execute(DocumentDatabase database)
		{
			database.ApplyPatch(Key, Etag, Patches, TransactionInformation);
		}
#endif
		public JObject ToJson()
		{
			return new JObject(
				new JProperty("Key", Key),
				new JProperty("Method", Method),
				new JProperty("Etag", Etag == null ? null : new JValue(Etag.ToString())),
				new JProperty("Patches", new JArray(Patches.Select(x=>x.ToJson())))
				);
		}
	}
}