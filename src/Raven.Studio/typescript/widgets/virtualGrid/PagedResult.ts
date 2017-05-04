// The results the new Virtual Grid is expecting.
interface PagedResult {
    skip: number;
    take: number;
    items: any[];
    totalCount: number;
}

export = PagedResult;