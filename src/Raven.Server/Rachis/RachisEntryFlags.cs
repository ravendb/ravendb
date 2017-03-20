namespace Raven.Server.Rachis
{
    public enum RachisEntryFlags
    {
        Invalid,
        Noop,// first commit in every term
        Topology,
        StateMachineCommand
    }
}