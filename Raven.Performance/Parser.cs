using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace Raven.Performance
{
	internal class Parser
	{
		private readonly List<Tuple<Regex, Action<Disk, MatchCollection>>> actions =
			new List<Tuple<Regex, Action<Disk, MatchCollection>>>();

		public Parser()
		{
			Add(@"^\#\s+xmcd", (disk, collection) =>
			{
				if (collection.Count == 0)
					throw new InvalidDataException("Not an XMCD file");
			});

			Add(@"^\# \s* Track \s+ frame \s+ offsets \s*: \s* \n (^\# \s* (\d+) \s* \n)+", (disk, collection) =>
			{
				foreach (Capture capture in collection[0].Groups[2].Captures)
				{
					disk.TrackFramesOffsets.Add(int.Parse(capture.Value));
				}
			});

			Add(@"Disc \s+ length \s*: \s* (\d+)", (disk, collection) =>
			                                       disk.DiskLength = int.Parse(collection[0].Groups[1].Value)
				);

			Add("DISCID=(.+)", (disk, collection) =>
			{
				var strings = collection[0].Groups[1].Value.Split(new[] {","},
				                                                  StringSplitOptions.RemoveEmptyEntries);
				disk.DiskIds.AddRange(strings.Select(x => x.Trim()));
			});

			Add("DTITLE=(.+)", (disk, collection) =>
			{
				var parts = collection[0].Groups[1].Value.Split(new[] {"/"}, 2, StringSplitOptions.RemoveEmptyEntries);
				if (parts.Length == 2)
				{
					disk.Artist = parts[0].Trim();
					disk.Title = parts[1].Trim();
				}
				else
				{
					disk.Title = parts[0].Trim();
				}
			});

			Add(@"DYEAR=(\d+)", (disk, collection) =>
			{
				if (collection.Count == 0)
					return;
				var value = collection[0].Groups[1].Value;
				if (value.Length > 4) // there is data like this
				{
					value = value.Substring(value.Length - 4);
				}
				disk.Year = int.Parse(value);
			}
				);

			Add(@"DGENRE=(.+)", (disk, collection) =>
			{
				if (collection.Count == 0)
					return;
				disk.Genre = collection[0].Groups[1].Value.Trim();
			}
				);

			Add(@"TTITLE\d+=(.+)", (disk, collection) =>
			{
				foreach (Match match in collection)
				{
					disk.Tracks.Add(match.Groups[1].Value.Trim());
				}
			});

			Add(@"(EXTD\d*)=(.+)", (disk, collection) =>
			{
				foreach (Match match in collection)
				{
					var key = match.Groups[1].Value;
					string value;
					if (disk.Attributes.TryGetValue(key, out value))
					{
						disk.Attributes[key] = value + match.Groups[2].Value.Trim();
					}
					else
					{
						disk.Attributes[key] = match.Groups[2].Value.Trim();
					}
				}
			});
		}

		private void Add(string regex, Action<Disk, MatchCollection> action)
		{
			var key = new Regex(regex,
			                    RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace |
			                    RegexOptions.Multiline);
			actions.Add(Tuple.Create(key, action));
		}

		public Disk Parse(string text)
		{
			var disk = new Disk();
			foreach (var action in actions)
			{
				var collection = action.Item1.Matches(text);
				try
				{
					action.Item2(disk, collection);
				}
				catch (Exception e)
				{
					Console.WriteLine();
					Console.WriteLine(text);
					Console.WriteLine(action.Item1);
					Console.WriteLine(e);
					throw;
				}
			}

			return disk;
		}
	}
}
