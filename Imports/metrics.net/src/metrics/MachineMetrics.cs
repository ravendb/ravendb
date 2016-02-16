using System;

namespace metrics
{
    /// <summary>
    /// A convenience class for installing global, machine-level metrics
    /// <seealso href="http://technet.microsoft.com/en-us/library/cc768048.aspx#XSLTsection132121120120" />
    /// <seealso href="http://msdn.microsoft.com/en-us/library/w8f5kw2e%28v=VS.71%29.aspx" />
    /// </summary>
    public  class MachineMetrics
    {
        private const string TotalInstance = "_Total";
        private const string GlobalInstance = "_Global_";
        private  Metrics _metrix = new Metrics();
        
        public  void InstallAll()
        {
        }

        

        //_Global_:.NET CLR Memory:# Gen 0 Collections
        //_Global_:.NET CLR Memory:# Gen 1 Collections
        //_Global_:.NET CLR Memory:# Gen 2 Collections
        //_Global_:.NET CLR Memory:Promoted Memory from Gen 0
        //_Global_:.NET CLR Memory:Promoted Memory from Gen 1
        //_Global_:.NET CLR Memory:Gen 0 Promoted Bytes/Sec
        //_Global_:.NET CLR Memory:Gen 1 Promoted Bytes/Sec
        //_Global_:.NET CLR Memory:Promoted Finalization-Memory from Gen 0
        //_Global_:.NET CLR Memory:Process ID
        //_Global_:.NET CLR Memory:Gen 0 heap size
        //_Global_:.NET CLR Memory:Gen 1 heap size
        //_Global_:.NET CLR Memory:Gen 2 heap size
        //_Global_:.NET CLR Memory:Large Object Heap size
        //_Global_:.NET CLR Memory:Finalization Survivors
        //_Global_:.NET CLR Memory:# GC Handles
        //_Global_:.NET CLR Memory:Allocated Bytes/sec
        //_Global_:.NET CLR Memory:# Induced GC
        //_Global_:.NET CLR Memory:% Time in GC
        //_Global_:.NET CLR Memory:Not Displayed
        //_Global_:.NET CLR Memory:# Bytes in all Heaps
        //_Global_:.NET CLR Memory:# Total committed Bytes
        //_Global_:.NET CLR Memory:# Total reserved Bytes
        //_Global_:.NET CLR Memory:# of Pinned Objects
        //_Global_:.NET CLR Memory:# of Sink Blocks in use
        public  void InstallCLRMemory()
        {
            throw new NotImplementedException();
        }
    }
}