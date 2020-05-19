using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Cluster
{
    public class VersionValidation : RavenTestBase
    {
        public VersionValidation(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AllClusterCommandsHasVersion()
        {
            List<Exception> exceptions = new List<Exception>();
            var assembly = typeof(Raven.Server.ServerWide.ServerStore).Assembly;
            foreach (var type in assembly.GetTypes())
            {
                var typeInfo = type;
                if (typeInfo.IsAbstract)
                    continue;

                if (typeInfo.IsSubclassOf(typeof(CommandBase)) == false)
                    continue;

                if (ClusterCommandsVersionManager.ClusterCommandsVersions.TryGetValue(type.Name, out int _))
                    continue;

                //Console.WriteLine($"[nameof({type.Name})] = 410,");

                exceptions.Add(new InvalidOperationException(
                    $"Missing version in '{nameof(ClusterCommandsVersionManager)}.{nameof(ClusterCommandsVersionManager.ClusterCommandsVersions)}' for the command '{type.Name}'."));
            }

            if (exceptions.Count == 0)
                return;

            throw new AggregateException(exceptions);
        }

        [Fact]
        public void AllClusterCommandHasCtor()
        {
            List<Exception> exceptions = new List<Exception>();

            var assembly = typeof(Raven.Server.ServerWide.ServerStore).Assembly;
            foreach (var type in assembly.GetTypes())
            {
                var typeInfo = type;
                if (typeInfo.IsAbstract)
                    continue;

                if (typeInfo.IsSubclassOf(typeof(CommandBase)) == false)
                    continue;

                var ctors = type
                    .GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

                if(ctors.Length > 1)
                    continue;

                var ctor = ctors.FirstOrDefault(x => x.GetParameters().Length == 0);

                if (ctor == null)
                {
                    exceptions.Add(new InvalidOperationException(
                        $"Missing ctor for de-serialization in '{type.Name}' command."));
                    continue;
                }

                var instance = (CommandBase)Activator.CreateInstance(type);

                if (instance.UniqueRequestId == null)
                    exceptions.Add(new InvalidOperationException(
                        $"Missing ctor with {nameof(CommandBase.RaftCommandIndex)} in '{type.Name}' command."));
            }

            if (exceptions.Count == 0)
                return;

            throw new AggregateException(exceptions);
        }
    }
}
