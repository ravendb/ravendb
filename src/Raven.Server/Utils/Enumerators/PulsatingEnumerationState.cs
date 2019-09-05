namespace Raven.Server.Utils.Enumerators
{
    public abstract class PulsatingEnumerationState<T>
    {
        public abstract void OnMoveNext(T current);

        public abstract bool ShouldPulseTransaction();
    }
}
