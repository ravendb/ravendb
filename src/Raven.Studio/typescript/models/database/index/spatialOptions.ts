/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class spatialOptions {
    maxTreeLevel = ko.observable<number>();
    minX = ko.observable<number>();
    maxX = ko.observable<number>();
    minY = ko.observable<number>();
    maxY = ko.observable<number>();
    strategy = ko.observable<Raven.Client.Documents.Indexes.Spatial.SpatialSearchStrategy>();
    type = ko.observable<Raven.Client.Documents.Indexes.Spatial.SpatialFieldType>();
    units = ko.observable<Raven.Client.Documents.Indexes.Spatial.SpatialUnits>();
    
    precision: KnockoutComputed<string>;
    showPrecision: KnockoutComputed<boolean>;

    availableStrategies = ko.observableArray<string>();
    canSpecifyUnits: KnockoutComputed<boolean>;
    canSpecifyTreeLevel: KnockoutComputed<boolean>;
    canSpecifyCoordinates: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Indexes.Spatial.SpatialOptions) {
        this.type(dto.Type);
        this.strategy(dto.Strategy);
        this.maxTreeLevel(dto.MaxTreeLevel);
        this.minX(dto.MinX);
        this.maxX(dto.MaxX);
        this.minY(dto.MinY);
        this.maxY(dto.MaxY);
        this.units(dto.Units);

        this.availableStrategies(this.getAvailableStrategies());
        this.canSpecifyUnits = ko.pureComputed(() => this.type() === "Geography");
        this.canSpecifyTreeLevel = ko.pureComputed(() => this.strategy() !== "BoundingBox");
        this.precision = ko.pureComputed(() => this.getPrecisionString());
        this.canSpecifyCoordinates = ko.pureComputed(() => this.type() === "Cartesian");
        this.type.subscribe(newType => {
            this.resetCoordinates();
            const availableStrategies = this.getAvailableStrategies(); 
            if (!_.includes(availableStrategies, this.strategy())) {
                this.strategy(availableStrategies[0]);
            }
            this.availableStrategies(availableStrategies);
        });
        
        this.strategy.subscribe(newStrategy => this.updateMaxTreeLevelFromStrategy(newStrategy));

        this.maxTreeLevel.extend({
            min: 0
        });

        this.validationGroup = ko.validatedObservable({
            maxTreeLevel: this.maxTreeLevel
        });

        this.showPrecision = ko.pureComputed(() => {
            return this.strategy() !== 'BoundingBox';
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.type,
            this.strategy,
            this.maxTreeLevel,
            this.minX,
            this.maxX,
            this.minY,
            this.maxY,
            this.units
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    toDto(): Raven.Client.Documents.Indexes.Spatial.SpatialOptions {
        return {
            Type: this.type(),
            Strategy: this.strategy(),
            MaxTreeLevel: this.maxTreeLevel(),
            MinX: this.minX(),
            MaxX: this.maxX(),
            MinY: this.minY(),
            MaxY: this.maxY(),
            Units: this.units()
        };
    }

    static empty(): spatialOptions {
        const dto: Raven.Client.Documents.Indexes.Spatial.SpatialOptions = {
            Type: "Geography",
            MaxTreeLevel: 9,
            MinX: -180,
            MaxX: 180,
            MinY: -90,
            MaxY: 90,
            Strategy: "GeohashPrefixTree",
            Units: "Kilometers"
        };
        
        return new spatialOptions(dto);
    }

    private resetCoordinates() {
        this.minX(-180);
        this.maxX(180);
        this.minY(-90);
        this.maxY(90);
    }

    private getPrecisionString(): string {
        const strategy = this.strategy();
        const minX = this.minX();
        const maxX = this.maxX();
        const minY = this.minY();
        const maxY = this.maxY();
        const maxTreeLevel = this.maxTreeLevel();
        const type = this.type();
        const units = this.units();

        if (strategy === "BoundingBox") {
            return "";
        }

        let x = maxX - minX;
        let y = maxY - minY;
        for (let i = 0; i < maxTreeLevel; i++) {
            if (strategy === "GeohashPrefixTree") {
                if (i % 2 == 0) {
                    x /= 8;
                    y /= 4;
                }
                else {
                    x /= 4;
                    y /= 8;
                }
            }
            else if (strategy === "QuadPrefixTree") {
                x /= 2;
                y /= 2;
            }
        }

        if (type === "Geography") {
            const earthMeanRadiusKm = 6371.0087714;
            const milesToKm = 1.60934;

            let factor = (earthMeanRadiusKm * Math.PI * 2) / 360;
            x = x * factor;
            y = y * factor;
            if (units === "Miles") {
                x /= milesToKm;
                y /= milesToKm;
            }

            return "Precision at equator; X: " + x.toFixed(6) + ", Y: " + y.toFixed(6) + " " + units.toLowerCase();
        } else {
            return "Precision; X: " + x.toFixed(6) + ", Y: " + y.toFixed(6);
        }
    }

    private getAvailableStrategies(): Raven.Client.Documents.Indexes.Spatial.SpatialSearchStrategy[] {
        if (this.type() === "Geography") {
            return ["GeohashPrefixTree", "QuadPrefixTree", "BoundingBox"];
        } else {
            return ["QuadPrefixTree", "BoundingBox"];
        }
    }

    private updateMaxTreeLevelFromStrategy(strategy: Raven.Client.Documents.Indexes.Spatial.SpatialSearchStrategy) {
        if (strategy === "GeohashPrefixTree") {
            this.maxTreeLevel(9);
        } else if (strategy === "QuadPrefixTree") {
            this.maxTreeLevel(23);
        }
    }
}

export = spatialOptions; 
