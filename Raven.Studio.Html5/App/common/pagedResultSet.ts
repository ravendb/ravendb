class pagedResultSet {
	constructor(public items: Array<any>, public totalResultCount: number, public additionalResultInfo?: any) {
	}
}

export = pagedResultSet;