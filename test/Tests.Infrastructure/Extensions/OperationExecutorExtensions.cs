using System;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure.Utils;

namespace Tests.Infrastructure.Extensions;

public static class OperationExecutorExtensions
{
    public static IMaintenanceOperationExecutorReadTester<TResult> ForTesting<TResult>(this MaintenanceOperationExecutor executor, Func<IMaintenanceOperation<TResult>> factory)
    {
        return new MaintenanceOperationExecutorTester<TResult>(executor, factory);
    }

    public static IMaintenanceOperationExecutorActionTester ForTesting(this MaintenanceOperationExecutor executor, Func<IMaintenanceOperation> factory)
    {
        return new MaintenanceOperationExecutorTester<object>(executor, factory);
    }
}
