using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Replication
{
    public class DefaultConflictResolver
    {
        private int _version = -1;
        public int Version => _version;

        private Guid _dbid;
        public Guid Dbid
        {
            get { return _dbid; }
            set
            {
                Interlocked.Increment(ref _version);
                _dbid = value;
                Save();
            }
        }

        private readonly DocumentsOperationContext _context;

        public DefaultConflictResolver(DocumentDatabase database)
        {
            database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
        }

        public bool HasLeader()
        {
            return _version > -1;
        }

        public void ParseAndUpdate(string guidStr, string versionStr)
        {
            int version;
            Guid guid;
            if (!Int32.TryParse(versionStr, out version))
            {
                throw new InvalidOperationException($"Faild to parse the resolver version. {versionStr}");
            }
            if (!Guid.TryParse(guidStr, out guid))
            {
                throw new InvalidOperationException($"Faild to parse the resolver database Id. {guidStr}");
            }
            if (!TryUpdate(guid, version))
            {
                throw new InvalidOperationException($"Resolver versions are conflicted. Same version {version}, but different resolvers {guid} and {_dbid}");
            }
        }

        public bool TryUpdate(Guid guid, int version)
        {
            if (version > _version)
            {
                _dbid = guid;
                _version = version;
                Save();
                //TODO: start new background task to resolve existing conflicts.
                return true;
            }
            // we have a problem if versions are equal, but not the uid.
            return !(version == _version && guid != _dbid);
        }

        public void Save()
        {
            using (var tx = _context.OpenWriteTransaction())
            {
                var tree = _context.Transaction.InnerTransaction.CreateTree("Resolver");
                tree.Add("MyResolver", Serialize());
                tx.Commit();
            }
        }

        public void Load()
        {
            using (_context.OpenReadTransaction())
            using (var memoryStream = new MemoryStream())
            {
                var tree = _context.Transaction.InnerTransaction.ReadTree("Resolver");
                var data = tree.Read("MyResolver");
                if (data == null)
                    return;
                data.Reader.CopyTo(memoryStream);
                Deserialize(memoryStream.ToArray());
            }
        }

        private byte[] Serialize()
        {
            using (var m = new MemoryStream())
            using (var writer = new BinaryWriter(m))
            {
                writer.Write(Version);
                writer.Write(Dbid.ToString());
                return m.ToArray();
            }
        }

        private void Deserialize(byte[] data)
        {
            using (var m = new MemoryStream(data))
            using (var reader = new BinaryReader(m))
            {
                _version = reader.ReadInt32();
                _dbid = new Guid(reader.ReadString());
            }
        }

        public void Dispose()
        {
            _context.Dispose();
        }
    }
    
}
