/// <reference path="../tsd.d.ts"/>

interface queryResultDto {
    Results: any[];
    Includes: any[];
}

interface canActivateResultDto {
    redirect?: string;
    can?: boolean;   
}

type menuItemType = "separator" | "intermediate" | "leaf";

interface menuItem {
    type: menuItemType;
}

type dynamicHashType = KnockoutObservable<string> | (() => string);

