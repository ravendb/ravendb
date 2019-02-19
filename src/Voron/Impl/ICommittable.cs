namespace Voron.Impl
{
    public interface ICommittable
    {
        bool RequiresParticipation { get; }
        void PrepareForCommit();
    }
}
