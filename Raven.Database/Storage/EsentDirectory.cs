using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Lucene.Net.Store;
using Microsoft.Isam.Esent.Interop;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Database.Storage
{
	public class EsentDirectory : Directory
	{
		private readonly TransactionalStorage transactionalStorage;
		private readonly string directory;

		public EsentDirectory(TransactionalStorage transactionalStorage, string directory)
		{
			this.transactionalStorage = transactionalStorage;
			this.directory = directory;

			base.SetLockFactory(new EsentLockFactory(transactionalStorage, directory));
		}

		[Obsolete("For some Directory implementations (FSDirectory}, and its subclasses), this method silently filters its results to include only index files.  Please use ListAll instead, which does no filtering. ")]
		public override string[] List()
		{
			string[] results = null;
			transactionalStorage.Batch(actions =>
			{
				results = actions.ListFilesInDirectory(directory);
			});
			return results;
		}

		public override bool FileExists(string name)
		{
			bool exists = false;
			transactionalStorage.Batch(actions =>
			{
				exists = actions.FileExistsInDirectory(directory, name);
			});
			return exists;
		}

		public override long FileModified(string name)
		{
			int version = -1;
			transactionalStorage.Batch(actions =>
			{
				version = actions.GetVersionOfFileInDirectory(directory, name);
			});
			return version;
		}

		public override void TouchFile(string name)
		{
			transactionalStorage.Batch(actions => actions.TouchFileInDirectory(directory, name));
		}

		public override void DeleteFile(string name)
		{
			transactionalStorage.Batch(actions => actions.DeleteFileInDirectory(directory, name));
		}

		[Obsolete]
		public override void RenameFile(string from, string to)
		{
			transactionalStorage.Batch(actions => actions.RenameFileInDirectory(directory, from, to));
		}

		public override long FileLength(string name)
		{
			long length = 0;
			transactionalStorage.Batch(actions =>
			{
				length = actions.GetLengthOfFileInDirectory(directory, name);
			});
			return length;
		}

		public override IndexOutput CreateOutput(string name)
		{
			var batch = transactionalStorage.INTERNAL_METHOD_GetCurrentBatch();
			if (batch.FileExistsInDirectory(directory, name) == false)// SIDE EFFECT: move the current row
				batch.CreateFileInDirectory(directory, name);
			var bookmark = Api.GetBookmark(batch.Session, batch.Directories);
			return new EsentIndexOutput(new ColumnStream(batch.Session, batch.Directories, transactionalStorage.TableColumnsCache.DirectoriesColumns["data"]),
				() => Api.JetGotoBookmark(batch.Session, batch.Directories, bookmark, bookmark.Length),
				() =>
				{
					Api.JetGotoBookmark(batch.Session, batch.Directories, bookmark, bookmark.Length);
					return new Update(batch.Session, batch.Directories, JET_prep.Replace);
				});
		}

		public override IndexInput OpenInput(string name)
		{
			var batch = transactionalStorage.INTERNAL_METHOD_GetCurrentBatch();
			if (batch.FileExistsInDirectory(directory, name) == false)
				throw new FileNotFoundException(name + " in " + directory);
			return new EsentIndexInput(transactionalStorage, directory, name);
		}

		public override void Close()
		{
		}

		public class EsentIndexInput : BufferedIndexInput
		{
			private readonly TransactionalStorage transactionalStorage;
			private readonly string directory;
			private readonly string name;
			private long position;

			public EsentIndexInput(TransactionalStorage transactionalStorage,string directory, string name)
			{
				this.transactionalStorage = transactionalStorage;
				this.directory = directory;
				this.name = name;
			}

			public override void Close()
			{
			}

			public override long Length()
			{
				var batch = transactionalStorage.INTERNAL_METHOD_GetCurrentBatch();
				return batch.GetLengthOfFileInDirectory(directory, name);
			}

			public override void ReadInternal(byte[] b, int offset, int length)
			{
				var batch = transactionalStorage.INTERNAL_METHOD_GetCurrentBatch();
				position += batch.ReadFromFileInDirectory(directory, name, position, b, offset, length);
			}

			public override void SeekInternal(long pos)
			{
				position = pos;
			}
		}

		public class EsentIndexOutput : BufferedIndexOutput
		{
			private readonly Stream stream;
			private readonly Action moveToRecord;
			private readonly Func<Update> moveToRecordAndStartUpdate;

			public EsentIndexOutput(Stream stream, Action moveToRecord, Func<Update> moveToRecordAndStartUpdate)
			{
				this.stream = stream;
				this.moveToRecord = moveToRecord;
				this.moveToRecordAndStartUpdate = moveToRecordAndStartUpdate;
			}

			public override void FlushBuffer(byte[] b, int offset, int len)
			{
				var update = moveToRecordAndStartUpdate();
				stream.Write(b, offset, len);
				stream.Flush();
				update.Save();
			}

			public override long Length()
			{
				moveToRecord();
				return stream.Length;
			}

			public override void Seek(long pos)
			{
				moveToRecord();
				base.Seek(pos);
				stream.Seek(pos, SeekOrigin.Begin);
			}

			public override void Close()
			{
				moveToRecord();
				base.Close();
				stream.Close();
			}
		}

		public class EsentLockFactory : LockFactory
		{
			private readonly TransactionalStorage transactionalStorage;
			private readonly string directory;

			public EsentLockFactory(TransactionalStorage transactionalStorage, string directory)
			{
				this.transactionalStorage = transactionalStorage;
				this.directory = directory;
			}

			public override Lock MakeLock(string lockName)
			{
				return new EsentLock(transactionalStorage.INTERNAL_METHOD_GetCurrentBatch(), directory, lockName);
			}

			public override void ClearLock(string lockName)
			{
				transactionalStorage.INTERNAL_METHOD_GetCurrentBatch().DeleteFileInDirectory(directory, lockName);
			}
		}

		public class EsentLock : Lock
		{
			private readonly string lockName;
			private readonly DocumentStorageActions batch;
			private readonly string directory;

			public EsentLock(DocumentStorageActions batch, string directory, string lockName)
			{
				this.lockName = lockName;
				this.batch = batch;
				this.directory = directory;
			}

			public override bool Obtain()
			{
				bool alreadyExists = batch.FileExistsInDirectory(directory, lockName);
				if (alreadyExists == false)
					batch.CreateFileInDirectory(directory, lockName);
				return alreadyExists == false;
			}

			public override void Release()
			{
				batch.DeleteFileInDirectory(directory, lockName);
			}

			public override bool IsLocked()
			{
				return batch.FileExistsInDirectory(directory, lockName);
			}
		}
	}
}