import genUtils from "common/generalUtils";
import { descending } from "d3-array";
import { hierarchy, HierarchyNode } from "d3-hierarchy";

export interface StorageReportItem {
    name: string;
    type: string;
    showType: boolean;
    size: number;
    internalChildren: StorageReportItem[];
    isGrouped?: boolean;
    customSizeProvider?: (header: boolean) => { title: string; text: string };
    pageCount?: number;
    numberOfEntries?: number;
    recyclableJournal?: boolean;
}

export function mapReport(reportItem: detailedSystemStorageReportItemDto): StorageReportItem {
    const dataFile = mapDataFile(reportItem.Report);
    const journals = mapJournals(reportItem.Report);
    const tempFiles = mapTempFiles(reportItem.Report);

    const result: StorageReportItem = {
        name: reportItem.Environment,
        type: reportItem.Type.toLowerCase(),
        showType: true,
        size: dataFile.size + journals.size + tempFiles.size,
        internalChildren: [dataFile, journals, tempFiles],
    };

    sortBySize(result);
    return result;
}

function mapDataFile(report: Voron.Debugging.DetailedStorageReport): StorageReportItem {
    const dataFile = report.DataFile;

    const tables = mapTables(report.Tables);
    const trees = mapTrees(report.Trees, "Trees");

    const freeSpace: StorageReportItem = {
        name: "Free",
        type: "free",
        showType: false,
        size: report.DataFile.FreeSpaceInBytes,
        internalChildren: [],
    };
    const preallocatedBuffers = mapPreAllocatedBuffers(report.PreAllocatedBuffers);

    return {
        name: "Datafile",
        type: "data",
        showType: false,
        size: dataFile.AllocatedSpaceInBytes,
        internalChildren: [tables, trees, freeSpace, preallocatedBuffers],
    } satisfies StorageReportItem;
}

function mapPreAllocatedBuffers(buffersReport: Voron.Debugging.PreAllocatedBuffersReport): StorageReportItem {
    const allocationTree = mapTree(buffersReport.AllocationTree);
    const buffersSpace: StorageReportItem = {
        name: "Pre Allocated Buffers Space",
        type: "reserved",
        showType: false,
        size: buffersReport.PreAllocatedBuffersSpaceInBytes,
        pageCount: buffersReport.NumberOfPreAllocatedPages,
        internalChildren: [],
    };

    return {
        name: "Pre Allocated Buffers",
        type: "reserved",
        showType: false,
        size: buffersReport.AllocatedSpaceInBytes,
        internalChildren: [allocationTree, buffersSpace],
        customSizeProvider: (header: boolean) => {
            const allocatedSizeFormatted = genUtils.formatBytesToSize(buffersReport.AllocatedSpaceInBytes);
            if (header) {
                return { title: undefined, text: allocatedSizeFormatted };
            }
            const originalSizeFormatted = genUtils.formatBytesToSize(buffersReport.OriginallyAllocatedSpaceInBytes);
            return {
                title: `${allocatedSizeFormatted} available out of ${originalSizeFormatted} reserved`,
                text: `${allocatedSizeFormatted} (out of ${originalSizeFormatted})`,
            };
        },
    } satisfies StorageReportItem;
}

function mapTables(tables: Voron.Data.Tables.TableReport[]): StorageReportItem {
    const mappedTables = tables.map((x) => mapTable(x));

    return {
        name: "Tables",
        type: "tables",
        showType: false,
        size: mappedTables.reduce((p, c) => p + c.size, 0),
        internalChildren: mappedTables,
    } satisfies StorageReportItem;
}

function mapTable(table: Voron.Data.Tables.TableReport): StorageReportItem {
    const structure = mapTrees(table.Structure, "Structure");

    const data: StorageReportItem = {
        name: "Table Data",
        type: "table_data",
        showType: false,
        size: table.DataSizeInBytes,
        internalChildren: [],
    };
    const indexes = mapTrees(table.Indexes, "Indexes");
    const preallocatedBuffers = mapPreAllocatedBuffers(table.PreAllocatedBuffers);

    return {
        name: table.Name,
        type: "table",
        showType: true,
        size: table.AllocatedSpaceInBytes,
        internalChildren: [structure, data, indexes, preallocatedBuffers],
        numberOfEntries: table.NumberOfEntries,
    } satisfies StorageReportItem;
}

function mapTrees(trees: Voron.Debugging.TreeReport[], name: string): StorageReportItem {
    return {
        name,
        type: name.toLowerCase(),
        showType: false,
        size: trees.reduce((p, c) => p + c.AllocatedSpaceInBytes, 0),
        internalChildren: trees.map((x) => mapTree(x)),
    };
}

function mapTree(tree: Voron.Debugging.TreeReport): StorageReportItem {
    const children = tree.Streams?.Streams ? tree.Streams.Streams.map((x) => mapStream(x)) : [];
    return {
        name: tree.Name,
        type: "tree",
        showType: true,
        size: tree.AllocatedSpaceInBytes,
        internalChildren: children,
        pageCount: tree.PageCount,
        numberOfEntries: tree.NumberOfEntries,
    };
}

function mapStream(stream: Voron.Debugging.StreamDetails): StorageReportItem {
    return {
        name: stream.Name,
        type: "stream",
        showType: false,
        size: stream.AllocatedSpaceInBytes,
        internalChildren: [],
        customSizeProvider: (header: boolean) => {
            const allocatedSizeFormatted = genUtils.formatBytesToSize(stream.AllocatedSpaceInBytes);
            if (header) {
                return { title: undefined, text: allocatedSizeFormatted };
            }
            const length = genUtils.formatBytesToSize(stream.Length);
            return {
                title: `stream length: ${length} / total allocation: ${allocatedSizeFormatted}`,
                text: `${length} / ${allocatedSizeFormatted}`,
            };
        },
    };
}

function mapJournals(report: Voron.Debugging.DetailedStorageReport): StorageReportItem {
    const journals = report.Journals.Journals;

    const mappedJournals = journals.map(
        (journal): StorageReportItem => ({
            name: "Journal #" + journal.Number,
            type: "journal",
            showType: false,
            size: journal.AllocatedSpaceInBytes,
            internalChildren: [],
        })
    );

    return {
        name: "Journals",
        type: "journals",
        showType: false,
        size: mappedJournals.reduce((p, c) => p + c.size, 0),
        internalChildren: mappedJournals,
    };
}

function mapTempFiles(report: Voron.Debugging.DetailedStorageReport): StorageReportItem {
    const tempFiles = report.TempBuffers;

    const mappedTemps = tempFiles.map((temp) => {
        return {
            name: temp.Name,
            type: "temp",
            showType: false,
            size: temp.AllocatedSpaceInBytes,
            internalChildren: [],
            recyclableJournal: temp.Type === "RecyclableJournal",
        } satisfies StorageReportItem;
    });

    return {
        name: "Temporary Files",
        type: "tempFiles",
        showType: false,
        size: mappedTemps.reduce((p, c) => p + c.size, 0),
        internalChildren: mappedTemps,
    };
}

function sortBySize(node: StorageReportItem): void {
    if (node.internalChildren && node.internalChildren.length) {
        node.internalChildren.forEach(sortBySize);
        node.internalChildren.sort((a, b) => descending(a.size, b.size));
    }
}

export function createHierarchyWithValues(node: StorageReportItem, flat = false): HierarchyNode<StorageReportItem> {
    const childrenExtractor = flat
        ? (d: StorageReportItem) => (d === node ? d.internalChildren : [])
        : (d: StorageReportItem) => d.internalChildren;

    return hierarchy<StorageReportItem>(node, childrenExtractor).eachBefore((d) => {
        // we don't use sum here, as the values are already summed-up - instead update readonly property 'value'
        (d.value as number) = d.data.size;
    });
}
