namespace Raven.Client.Http
{
    public enum AggressiveCacheMode
    {
        Unknown = 0,
        TrackChanges = 1,
        TrackChangesAndDoBackgroundRefresh = 2, // TODO arek - NotImplementedException
        DoNotTrackChanges = 3
    }
}
