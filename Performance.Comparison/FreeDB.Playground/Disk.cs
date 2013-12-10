using System.Collections.Generic;

namespace XmcdParser
{
	public class Disk
	{
		public string Title { get; set; }
		public string Artist { get; set; }
		public int DiskLength { get; set; }
		public string Genre { get; set; }
		public int Year { get; set; }
		public List<string> DiskIds { get; set; }

		public List<int> TrackFramesOffsets { get; set; }
		public List<string> Tracks { get; set; }
		public Dictionary<string, string> Attributes { get; set; }
		public Disk()
		{
			TrackFramesOffsets = new List<int>();
			Tracks = new List<string>();
			DiskIds = new List<string>();
			Attributes = new Dictionary<string, string>();
		}
	}
}