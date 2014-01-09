using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    static class SmugglerApiFactory
    {
        public static SmugglerApiBase Create(SmugglerAction action, SmugglerOptions options, RavenConnectionStringOptions connectionStringOptions)
        {
            switch (action)
            {
                case SmugglerAction.Dryrun:
                    return new DryrunApi(options);                    

                case SmugglerAction.Repair:
                    return new RepairApi(options);

                default:
                    return new SmugglerApi(options, connectionStringOptions);
            }
        }
    }
}
