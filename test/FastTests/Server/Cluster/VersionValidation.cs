using System;
using System.Collections.Generic;
using System.Reflection;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Xunit;

namespace FastTests.Server.Cluster
{
    public class VersionValidation : RavenTestBase
    {
        [Fact]
        public void AllClusterCommandsHasVersion()
        {
            List<Exception> exceptions = new List<Exception>();
            var assembly = typeof(Raven.Server.ServerWide.ServerStore).GetTypeInfo().Assembly;
            foreach (var type in assembly.GetTypes())
            {
                var typeInfo = type.GetTypeInfo();
                if (typeInfo.IsAbstract)
                    continue;

                if (typeInfo.IsSubclassOf(typeof(CommandBase)) == false)
                    continue;

                if (ClusterCommandsVersionManager.ClusterCommandsVersions.TryGetValue(type.Name, out int _))
                    continue;
                exceptions.Add(new InvalidOperationException($"Missing version in '{nameof(ClusterCommandsVersionManager)}.{nameof(ClusterCommandsVersionManager.ClusterCommandsVersions)}' for the command '{type.Name}'."));
            }

            if (exceptions.Count == 0)
                return;

            throw new AggregateException(exceptions);
        }

    }
}
