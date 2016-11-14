using System;
using Raven.NewClient.Client.Connection;

namespace Raven.NewClient.Client.FileSystem.Connection
{
    public interface IAsyncFilesCommandsImpl : IAsyncFilesCommands
    {   
        string ServerUrl { get; }

        HttpJsonRequestFactory RequestFactory { get; }

        IFilesReplicationInformer ReplicationInformer { get; }
    }
}
