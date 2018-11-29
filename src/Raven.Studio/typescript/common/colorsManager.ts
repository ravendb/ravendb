/// <reference path="../../typings/tsd.d.ts"/>

class colorsManager {
    
    static setup(containerSelector: string, targetObject: any) {
        const $container = $(containerSelector);
        if (!$container.length) {
            throw new Error("Unable to find container. Selector used: " + containerSelector);
        }
        
        const $definitionsContainer = $(".color-definitions", $container);

        if (!$definitionsContainer.length) {
            throw new Error("Unable to find color definitions. Parent container: " + containerSelector);
        }
        
        const definitionsDom = $definitionsContainer[0];

        const readColors = colorsManager.domToObject(definitionsDom);
        
        colorsManager.assignColorProperties(readColors, targetObject, []);
    }
    
    private static assignColorProperties(source: any, target: any, path: string[]) {
        const sourceKeys = Object.keys(source);
        const targetKeys = Object.keys(target);
        
        const onlyInTarget = _.without(targetKeys, ...sourceKeys);
        const onlyInSource = _.without(sourceKeys, ...targetKeys);
        
        if (onlyInSource.length) {
            console.warn("Found missing color definitions (in TS file): " + onlyInSource.join(", ") + ", at path: /" + path.join("/"));
        }

        if (onlyInTarget.length) {
            console.warn("Found missing color definitions (in HTML file): " + onlyInTarget.join(", ") + ", at path: /" + path.join("/"));
        }
        
        targetKeys.forEach(key => {
            if (_.isObject(target[key])) {
                colorsManager.assignColorProperties(source[key], target[key], path.concat(key));
            } else {
                target[key] = source[key];
            }
        });
    }
    
    private static domToObject(node: HTMLElement) {
        return _.reduce(Array.from(node.children), (acc: any, n: HTMLElement) => {
            const nodeDataSet = n.dataset;
            const propertyName = nodeDataSet["property"] || n.className;
            
            if (n.children.length) {
                acc[propertyName] = colorsManager.domToObject(n as HTMLElement);
            } else {
                const styles = getComputedStyle(n);
                acc[propertyName] = styles.color;    
            }
            
            return acc;
        }, {});
    }
}

export = colorsManager;
