namespace Raven.Client.Documents.Changes;

public interface IOperationChanges<out TChange>
{
    /// <summary>
    /// Subscribe to changes for specified operation only.
    /// </summary>
    /// <returns></returns>
    IChangesObservable<TChange> ForOperationId(long operationId);

    /// <summary>
    /// Subscribe to change for all operation statuses.
    /// </summary>
    /// <returns></returns>
    IChangesObservable<TChange> ForAllOperations();
}
