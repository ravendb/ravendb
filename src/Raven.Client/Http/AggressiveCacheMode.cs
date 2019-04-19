namespace Raven.Client.Http
{
    public enum AggressiveCacheMode
    {
        TrackChanges = 0,
        TrackChangesAndDoBackgroundRefresh = 1, // TODO arek - NotImplementedException
        DoNotTrackChanges = 2
    }
}
