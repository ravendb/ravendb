// The results the new Virtual Grid is expecting.
interface pagedResult<T> {
    items: T[];
    totalResultCount: number;
    additionalResultInfo?: any; // Not used in the virtual grid, but preserved for compatibility with existing Raven Studio code.
}

export = pagedResult;
