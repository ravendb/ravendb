using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;

namespace Raven.Database.Data
{
	public static class CommandDataFactory
	{
		public static ICommandData CreateCommand(JObject jsonCommand, TransactionInformation transactionInformation)
		{
			var key = jsonCommand["Key"].Value<string>();
			switch (jsonCommand.Value<string>("Method"))
			{
				case "PUT":
					return new PutCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						Document = jsonCommand["Document"] as JObject,
						Metadata = jsonCommand["Metadata"] as JObject,
						TransactionInformation = transactionInformation
					};
				case "DELETE":
					return new DeleteCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						TransactionInformation = transactionInformation
					};
				case "PATCH":
					return new PatchCommandData
					{
						Key = key,
						Etag = GetEtagFromCommand(jsonCommand),
						TransactionInformation = transactionInformation,
						Patches = jsonCommand
							.Value<JArray>("Patches")
							.Cast<JObject>()
							.Select(PatchRequest.FromJson)
							.ToArray<PatchRequest>()
					};
				default:
					throw new ArgumentException("Batching only supports PUT, PATCH and DELETE.");
			}
		}

		private static Guid? GetEtagFromCommand(JToken jsonCommand)
		{
			return jsonCommand["Etag"] != null && jsonCommand["Etag"].Value<string>() != null ? new Guid(jsonCommand["Etag"].Value<string>()) : (Guid?)null;
		}
	}
}