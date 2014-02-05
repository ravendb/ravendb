//-----------------------------------------------------------------------
// <copyright file="Command.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Munin
{
	public class Command
	{
		public byte[] Payload { get; set; }
		public int Size { get; set; }
		public RavenJToken Key { get; set; }
		public CommandType Type { get; set; }
		public long Position { get; set; }
		public int DictionaryId { get; set; }

		public Command()
		{
			Position = -1;
			Size = 0;
		}

		public override string ToString()
		{
			return Type + " " + Key.ToString(Formatting.None);
		}
	}
}