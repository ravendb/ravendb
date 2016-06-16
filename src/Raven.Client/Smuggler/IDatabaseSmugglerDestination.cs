using System.IO;

namespace Raven.Client.Smuggler
{
    public interface IDatabaseSmugglerDestination
    {
        Stream CreateStream();
    }
}