// The results the new Virtual Grid is expecting.
interface pagedResult<T> {
    skip: number;
    take: number;
    items: T[];
    totalCount: number;
}

export = pagedResult;
