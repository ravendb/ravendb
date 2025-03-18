using System.Collections.Generic;

namespace Tests.Infrastructure.InterversionTest
{
    public abstract class RavenServerLocator
    {
        public abstract string ServerPath { get; }
        public abstract string ServerUrl { get; }
        public abstract string DataDir { get; }

        public virtual string Command => ServerPath;

        public virtual string CommandArguments => string.Empty;

        public abstract Dictionary<string, string> EnvironmentVariables { get; }
    }
}
