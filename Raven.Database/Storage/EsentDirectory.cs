using System;
using System.IO;
using System.Text;
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

			SetLockFactory(new EsentLockFactory(this));
		}

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
			transactionalStorage.Batch(actions =>
			{
				if (actions.FileExistsInDirectory(directory, name) == false) 
					actions.CreateFileInDirectory(directory, name);
			});
			return OpenSession<IndexOutput>(name, true, (stream, action) => new EsentIndexOutput(stream, action));
		}

		public override IndexInput OpenInput(string name)
		{
				transactionalStorage.Batch(actions =>
			{
				if (actions.FileExistsInDirectory(directory, name) == false) 
					actions.CreateFileInDirectory(directory, name);
			});
				return OpenSession<IndexInput>(name, false, (stream, action) => new EsentIndexInput(stream, action));
		}

		private T OpenSession<T>(string name, bool update, Func<Stream, Action,T> create)
		{
			Table table = null;
			Transaction transaction = null;
			Session session = null;
			var tuple = transactionalStorage.OpenSession();
			try
			{
				transaction = tuple.Item2;
				session = tuple.Item1;
				table = new Table(session, tuple.Item3, "directories", OpenTableGrbit.None);
				Api.JetSetCurrentIndex(session, table, "by_index_and_name");
				Api.MakeKey(session, table, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, table, name, Encoding.Unicode, MakeKeyGrbit.None);
				if (Api.TrySeek(session, table, SeekGrbit.SeekEQ) == false)
					throw new InvalidOperationException("File " + name + " was not found in " + directory);

				Update updateOp = null;
				if(update)
					updateOp = new Update(session, table, JET_prep.Replace);
				
				return create(new ColumnStream(session, table, transactionalStorage.TableColumnsCache.DirectoriesColumns["data"]), () =>
				{
					if (updateOp != null)
					{
						updateOp.Save();
					}

					table.Dispose();
					Api.JetCloseDatabase(session, tuple.Item3,CloseDatabaseGrbit.None);
					transaction.Commit(CommitTransactionGrbit.LazyFlush);
					transaction.Dispose();
					session.Dispose();
				});
			}
			catch (Exception)
			{
				if (table != null)
					table.Dispose();
				Api.JetCloseDatabase(session, tuple.Item3, CloseDatabaseGrbit.None);
				if (transaction != null)
					transaction.Dispose();
				if (session != null)
					session.Dispose();
				throw;
			}
		}

		public override void Close()
		{
		}

		public class EsentIndexInput : BufferedIndexInput
		{
			private readonly Stream stream;
			private readonly Action onClose;

			public EsentIndexInput(Stream stream, Action onClose)
			{
				this.stream = stream;
				this.onClose = onClose;
			}

			public override void Close()
			{
				stream.Close();
				onClose();
			}

			public override long Length()
			{
				return stream.Length;
			}

			protected override void ReadInternal(byte[] b, int offset, int length)
			{
				stream.Read(b, offset, length);
			}

			protected override void SeekInternal(long pos)
			{
				stream.Seek(pos, SeekOrigin.Begin);
			}
		}

		public class EsentIndexOutput : BufferedIndexOutput
		{	
			private readonly Stream stream;
			private readonly Action onClose;

			public EsentIndexOutput(Stream stream, Action onClose)
			{
				this.stream = stream;
				this.onClose = onClose;
			}


			public override void FlushBuffer(byte[] b, int offset, int len)
			{
				stream.Write(b, offset, len);
			}

			public override long Length()
			{
				return stream.Length;
			}

			public override void Close()
			{
				base.Close();
				stream.Close();
				onClose();
			}
		}

		public class EsentLockFactory : LockFactory
		{
			private readonly EsentDirectory directory;

			public EsentLockFactory(EsentDirectory directory)
			{
				this.directory = directory;
			}

			public override Lock MakeLock(string lockName)
			{
				return new EsentLock(lockName, directory);
			}

			public override void ClearLock(string lockName)
			{
				directory.DeleteFile(lockName);
			}
		}

		public class EsentLock : Lock
		{
			private readonly string lockName;
			private readonly EsentDirectory directory;

			public EsentLock(string lockName, EsentDirectory directory)
			{
				this.lockName = lockName;
				this.directory = directory;
			}

			public override bool Obtain()
			{
				var indexOutput = directory.CreateOutput(lockName);
				indexOutput.WriteByte(1);
				indexOutput.Close();
				return true;
			}

			public override void Release()
			{
				directory.DeleteFile(lockName);
			}

			public override bool IsLocked()
			{
				return directory.FileExists(lockName);
			}
		}
	}
}