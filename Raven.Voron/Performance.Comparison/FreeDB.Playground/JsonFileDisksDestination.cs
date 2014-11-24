using System.IO;
using System.IO.Compression;
using Newtonsoft.Json;
using XmcdParser;

namespace FreeDB.Playground
{
	public class JsonFileDisksDestination : DisksDestination
	{
		private readonly GZipStream _stream;
		private readonly StreamWriter _writer;
		private readonly JsonSerializer _serializer = new JsonSerializer();

		public JsonFileDisksDestination()
		{
			_stream = new GZipStream(new FileStream("freedb.json.gzip", FileMode.CreateNew, FileAccess.ReadWrite), CompressionLevel.Optimal);
			_writer = new StreamWriter(_stream);
		}

		public override void Accept(Disk d)
		{
			_serializer.Serialize(new JsonTextWriter(_writer), d);
			_writer.WriteLine();
		}

		public override void Done()
		{
			_writer.Flush();
			_stream.Dispose();
		}
	}
}