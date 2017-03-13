/// <reference path="../../../typings/tsd.d.ts"/>
type selectionMode = "inclusive" | "exclusive";

interface virtualGridSelection<T> {

    /**
     * Selection mode
     */
    mode: selectionMode;

    /**
     * List of excluded nodes, filled-in only when mode = exclusive
     * Ex.
     * For collection: [1,2,3,4,5], when user selects all items and then uncheck [5], then:
     * mode = "excluding" and excluded = [5], so effective selection is: [1,2,3,4]
     */
    excluded: T[];

    /**
     * List of selected items, filled-in only when mode = inclusive
     */
    included: T[];

    /**
      * Effective number of selected items
      */
    count: number;

    /**
     * Total records
     */
    totalCount: number;

}

export = virtualGridSelection;