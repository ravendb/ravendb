#if !DNXCORE50
using System;
using System.Transactions;
using System.Collections.Generic;
using System.IO;

namespace Raven.Client.Document.DTC
{
    public class LocalDirectoryTransactionRecoveryStorage : ITransactionRecoveryStorageContext, ITransactionRecoveryStorage
    {
        private readonly string path;

        public LocalDirectoryTransactionRecoveryStorage(string path)
        {
            this.path = path;
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);
        }

        public void CreateFile(string name, Action<Stream> createFile)
        {
            name = Path.Combine(path, name);
            using (var file = File.Create(name + ".temp"))
            {
                createFile(file);
                file.Flush(true);
            }
            File.Move(name + ".temp", name);
        }

        public void DeleteFile(string name)
        {
            if (File.Exists(name) == false)
                return;
            File.Delete(name);
        }

        public IEnumerable<string> GetFileNames(string filter)
        {
            return Directory.GetFiles(path, filter);
        }

        public Stream OpenRead(string name)
        {
            return File.Open(name, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        }

        public byte[] GetRecoveryInformation(PreparingEnlistment preparingEnlistment)
        {
            return preparingEnlistment.RecoveryInformation();
        }

        public void Dispose()
        {
            
        }

        public ITransactionRecoveryStorageContext Create()
        {
            return this;
        }
    }
}
#endif