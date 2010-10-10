using System;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class GeneralStorageActions : IGeneralStorageActions
    {
        private readonly TableStorage storage;

        public GeneralStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public long GetNextIdentityValue(string name)
        {
            var result = storage.Identity.Read(name);
            if(result == null)
            {
                storage.Identity.Put(name, BitConverter.GetBytes(1));
                return 1;
            }
            else
            {
                var val = BitConverter.ToInt32(result.Data(), 0) + 1;
                storage.Identity.Put(name, BitConverter.GetBytes(val));
                return val;
            }
        }
    }
}