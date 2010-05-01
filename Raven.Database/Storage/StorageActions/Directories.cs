using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage
{
	public partial class DocumentStorageActions
	{
		public string[] ListFilesInDirectory(string directory)
		{
			if(SetIndexRangeForDirectory(directory)==false)
				return new string[0];

			var results = new List<string>();
			do
			{
				results.Add(Api.RetrieveColumnAsString(session, Directories, tableColumnsCache.DirectoriesColumns["name"],Encoding.Unicode));
			} while (Api.TryMoveNext(session, Directories));
			return results.ToArray();
		}

		private bool SetIndexRangeForDirectory(string directory)
		{
			Api.JetSetCurrentIndex(session, Directories, "by_index");
			Api.MakeKey(session, Directories, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if(Api.TrySeek(session, Directories, SeekGrbit.SeekEQ)==false)
				return false;
			Api.MakeKey(session, Directories, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, Directories, SetIndexRangeGrbit.RangeInclusive|SetIndexRangeGrbit.RangeUpperLimit);
			return true;
		}

		public bool FileExistsInDirectory(string directory, string name)
		{
			Api.JetSetCurrentIndex(session, Directories, "by_index_and_name");
			Api.MakeKey(session, Directories, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Directories, name, Encoding.Unicode, MakeKeyGrbit.None);
			return Api.TrySeek(session, Directories, SeekGrbit.SeekEQ);
			
		}

		public int GetVersionOfFileInDirectory(string directory, string name)
		{
			GotoFile(directory, name); 
			return Api.RetrieveColumnAsInt32(session, Directories, tableColumnsCache.DirectoriesColumns["version"]).Value;
		}

		public void TouchFileInDirectory(string directory, string name)
		{
			GotoFile(directory, name);

			using(var update = new Update(session,Directories,JET_prep.Replace))
			{
				update.Save();
			}
		}

		public void DeleteFileInDirectory(string directory, string name)
		{
			if (FileExistsInDirectory(directory, name) == false)
				return;
			Api.JetDelete(session, Directories);
		}

		public void RenameFileInDirectory(string directory, string src, string dest)
		{
			GotoFile(directory, src);
			using (var update = new Update(session, Directories, JET_prep.Replace))
			{
				Api.SetColumn(session, Directories, tableColumnsCache.DirectoriesColumns["name"], dest, Encoding.Unicode);
				update.Save();
			}
		}

		public long GetLengthOfFileInDirectory(string directory, string name)
		{
			GotoFile(directory, name);
			return Api.RetrieveColumnSize(session, Directories, tableColumnsCache.DirectoriesColumns["data"]).Value;
		}

		private void GotoFile(string directory, string name)
		{
			Api.JetSetCurrentIndex(session, Directories, "by_index_and_name");
			Api.MakeKey(session, Directories, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Directories, name, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, Directories, SeekGrbit.SeekEQ) == false)
				throw new InvalidOperationException(string.Format("File {0} was not found in {1} ", name, directory));
		}

		public void DeleteAllFilesInDirectory(string name)
		{
			if (SetIndexRangeForDirectory(name) == false)
				return;
			do
			{
				Api.JetDelete(session, Directories);
			} while (Api.TryMoveNext(session, Directories));
		}

		public void CreateFileInDirectory(string directory, string name)
		{
			var bookmark = new byte[SystemParameters.BookmarkMost];
			int size;
			using (var update = new Update(session, Directories, JET_prep.Insert))
			{
				Api.SetColumn(session, Directories, tableColumnsCache.DirectoriesColumns["index"], directory, Encoding.Unicode);
				Api.SetColumn(session, Directories, tableColumnsCache.DirectoriesColumns["name"], name, Encoding.Unicode);
				update.Save(bookmark,bookmark.Length, out size);
			}
			Api.JetGotoBookmark(session, Directories, bookmark, size);
		}

		public long ReadFromFileInDirectory(string directory, string name, long position, byte[] bytes, int offset, int length)
		{
			GotoFile(directory, name);
			using(var stream = new ColumnStream(session,Directories, tableColumnsCache.DirectoriesColumns["data"]))
			{
				stream.Position = position;
				return stream.Read(bytes, offset, length);
			}
		}
	}
}