using System;

namespace Raven.Server.Commercial;

public class UpgradeRequired
{
    public bool AllowDismiss { get; set; }

    public DateTime AllowDismissUntil { get; set; }
}
