namespace Raven.Server.Rachis
{
    public enum RachisEntryFlags
    {
        Noop,// first commit in every term
        Topology,
        StateMachineCommand
    }
}