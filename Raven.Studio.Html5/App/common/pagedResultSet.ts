class pagedResultSet {
	constructor(public items: Array<any>, public totalResultCount: number, private additionalResultInfo?: any) {
	}
}

export = pagedResultSet;