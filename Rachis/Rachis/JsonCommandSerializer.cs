using System;
using System.IO;
using Rachis.Commands;
using Rachis.Interfaces;

using Raven.Imports.Newtonsoft.Json;

namespace Rachis
{
	public class JsonCommandSerializer : ICommandSerializer
	{
		private readonly JsonSerializer _serializer;

		public JsonCommandSerializer()
		{
			_serializer = new JsonSerializer
			{
				TypeNameHandling = TypeNameHandling.Objects
			};
		}

		public byte[] Serialize(Command cmd)
		{
			using (var memoryStream = new MemoryStream())
			{
				using (var streamWriter = new StreamWriter(memoryStream))
				{
					_serializer.Serialize(streamWriter, cmd);
					streamWriter.Flush();
				}
				return memoryStream.ToArray();
			}
		}

		public Command Deserialize(byte[] cmd)
		{
			lock(this)
			using (var memoryStream = new MemoryStream(cmd))
			using (var streamReader = new StreamReader(memoryStream))
			{
				Command command;
				try
				{
					var jsonTextReader = new JsonTextReader(streamReader);
					var deserialized = _serializer.Deserialize(jsonTextReader);
					command = deserialized as Command;
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Failed to deserialize command. This is not supposed to happen and it is probably a bug",e);
				}

				if(command == null)
					throw new ArgumentException("JsonCommandSerializer should only receive Command implementations to deserialize","cmd");
				
				return command;
			}
		}
	}
}
