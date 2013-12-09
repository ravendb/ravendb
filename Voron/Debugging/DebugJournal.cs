using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Trees;

namespace Voron.Debugging
{
	public class DebugJournal : IDisposable
	{
		public class ActivityEntry
		{
		    public DebugActionType ActionType { get; private set; }

			public string TreeName { get; private set; }

			public Slice Key { get; private set; }

			public object Value { get; private set; }

			public Stream ValueStream
			{
				get
				{
					if(Value == null)
						return null;

					if (Value is String || Value is Slice)
						return new MemoryStream(Encoding.UTF8.GetBytes(Value.ToString()));

					if (Value is Stream)
						return Value as Stream;

					throw new InvalidOperationException("Value of activity entry is of unsupported type");
				}
			}

			public ActivityEntry(DebugActionType actionType, Slice key, string treeName, object value)
			{
				ActionType = actionType;
				Key = key;
				TreeName = treeName;
				Value = value;
			}

			public string ToCsvLine()
			{
				string entryValue = string.Empty;

				if (Value is String || Value is Slice)
					entryValue = Value.ToString();

				else if (Value is Stream && Value != Stream.Null)
				{
					var valueStream = (Stream)Value;
					valueStream.Position = 0;
					using (var reader = new StreamReader(Value as Stream))
						entryValue = reader.ReadToEnd();
				}
				else if (Value == Stream.Null)
					entryValue = null;

				return String.Format("{0},{1},{2},{3}",ActionType,TreeName,Key,entryValue);
			}

			public static ActivityEntry FromCsvLine(string csvLine)
			{
				var columnArray = csvLine.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).ToList();
				if (columnArray.Count != 4 && columnArray.Count != 2 && columnArray.Count != 3)
					throw new ArgumentException("invalid csv data - check that you do not have commas in data");

				try
				{
					if (columnArray[0] == DebugActionType.CreateTree.ToString())
					{
						var activityEntry = new ActivityEntry(
							GenericUtil.ParseEnum<DebugActionType>(columnArray[0]),
							Slice.Empty,
							columnArray[1],
							null);
						return activityEntry;
					}

					if (columnArray[0] == DebugActionType.Delete.ToString())
					{
						var activityEntry = new ActivityEntry(
							GenericUtil.ParseEnum<DebugActionType>(columnArray[0]),
							columnArray[2],
							columnArray[1],
							null);

						return activityEntry;
					}

					var entry = new ActivityEntry(
						GenericUtil.ParseEnum<DebugActionType>(columnArray[0]),
						columnArray[2],
						columnArray[1],
						columnArray[3]);

					return entry;
				}
				catch (Exception e)
				{
					throw new ArgumentException("Unable to parse the argument",e);
				}
			}
		}

		private StorageEnvironment _env;
		private FileStream _journalFileStream;
		private TextWriter _journalWriter;
		private const string FileExtension = ".djrs";

		public bool IsRecording { get; set; }

		public ConcurrentQueue<ActivityEntry> WriteQueue { get; private set; }

		public DebugJournal(string journalName, StorageEnvironment env, bool isRecordingByDefault = false)
		{
			_env = env;
			IsRecording = isRecordingByDefault;
			InitializeDebugJournal(journalName);
		}

		private void InitializeDebugJournal(string journalName)
		{
			Dispose();
			_journalFileStream = new FileStream(journalName + FileExtension, FileMode.OpenOrCreate, FileAccess.ReadWrite);
			_journalWriter = new StreamWriter(_journalFileStream, Encoding.UTF8);
			WriteQueue = new ConcurrentQueue<ActivityEntry>();
		}

		public void Load(string journalName)
		{
			InitializeDebugJournal(journalName);
			using (var journalReader = new StreamReader(_journalFileStream, Encoding.UTF8))
			{
				while (journalReader.Peek() >= 0)
				{
					var csvLine = journalReader.ReadLine();
					if(!string.IsNullOrWhiteSpace(csvLine))
						WriteQueue.Enqueue(ActivityEntry.FromCsvLine(csvLine));
				}
			}

		}


		public static DebugJournal FromFile(string journalName,StorageEnvironment env)
		{
			var newJournal = new DebugJournal(journalName,env);
			newJournal.Load(journalName);

			return newJournal;
		}
		
		[Conditional("DEBUG")]
		public void RecordAction(DebugActionType actionType, Slice key, string treeName, object value)
		{
			if (IsRecording)
			{
				var newAction = new ActivityEntry(actionType, key, treeName, value);
				WriteQueue.Enqueue(newAction);
				_journalWriter.WriteLine(newAction.ToCsvLine());
			}
		}

		[Conditional("DEBUG")]
		public void Flush()
		{
			try
			{
				_journalWriter.Flush();
			}
			catch (ObjectDisposedException)
			{
			}
		}

		public void Replay()
		{
			var wasDebugRecording = _env.IsDebugRecording;
			_env.IsDebugRecording = false;

			using (var writeBatch = new WriteBatch())
			{
				ActivityEntry entry;
				while (WriteQueue.TryDequeue(out entry))
				{
					switch (entry.ActionType)
					{
						case DebugActionType.Add:
							writeBatch.Add(entry.Key, entry.ValueStream, entry.TreeName);
							break;
						case DebugActionType.Delete:
							writeBatch.Delete(entry.Key, entry.TreeName);
							break;
						case DebugActionType.MultiAdd:
							writeBatch.MultiAdd(entry.Key, new Slice(Encoding.UTF8.GetBytes(entry.Value.ToString())), entry.TreeName);
							break;
						case DebugActionType.MultiDelete:
							writeBatch.MultiDelete(entry.Key, new Slice(Encoding.UTF8.GetBytes(entry.Value.ToString())), entry.TreeName);
							break;
						case DebugActionType.CreateTree:
							using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
							{
								_env.CreateTree(tx, entry.TreeName);
								tx.Commit();
							}
							break;
						default: //precaution against newly added action types
							throw new InvalidOperationException("unsupported tree action type");
					}
				}

				_env.Writer.Write(writeBatch);
			}

			_env.IsDebugRecording = wasDebugRecording; //restore the state as it was
		}

		public void Dispose()
		{
			try
			{
				if (_journalWriter != null)
					_journalWriter.Dispose();

				if (_journalFileStream != null)
					_journalFileStream.Dispose();
			}
			catch (ObjectDisposedException)
			{
			}
		}
	}
}
