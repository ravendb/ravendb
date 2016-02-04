# <a href="http://msdn.microsoft.com/en-us/library/ms684016.aspx" title="JetOSSnapshotPrepareInstance">JetOSSnapshotPrepareInstance</a>
while (<>) {
    if (/a href=\"([^\"]+)\" title=\"(\w+)\"/) {
        print "[url:$2|$1]\n";
    }
}